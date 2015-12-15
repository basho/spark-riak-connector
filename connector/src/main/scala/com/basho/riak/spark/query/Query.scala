/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.query

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.kv.{FetchValue, MultiFetch}
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.rdd.{BucketDef, ReadConf, RiakConnector}
import org.apache.spark.Logging
import org.apache.spark.metrics.RiakConnectorSource
import org.perf4j.log4j.Log4JStopWatch

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Generic Riak Query
 */
trait Query[T] extends Serializable with Logging{
  type ResultT = (Location, RiakObject)

  def bucket: BucketDef
  def readConf: ReadConf

  def nextChunk(nextToken: Option[_], session: RiakClient): (Option[T], Iterable[ResultT])
}

trait LocationQuery[T] extends Query[T] {
  private val dataBuffer: ArrayBuffer[ResultT] = new ArrayBuffer[ResultT](readConf.fetchSize)

  def nextLocationChunk(nextToken: Option[_], session: RiakClient): (Option[T], Iterable[Location])

  def nextChunk(token: Option[_], session: RiakClient): (Option[T], Iterable[ResultT]) = {
    //perf4j stop watches
    val fullChunkSw = new Log4JStopWatch()
    val lapSw = new Log4JStopWatch()

    //codahale timers
    val fullChunkCtx = RiakConnectorSource.instance.map(_.fbr2Full.time())
    val notFullChunkCtx = RiakConnectorSource.instance.map(_.fbr2NotFull.time())
    val locationsFull = RiakConnectorSource.instance.map(_.fbr2LocationsFull.time())
    val locationsNotFull = RiakConnectorSource.instance.map(_.fbr2LocationsNotFull.time())


    try {
      val r = nextLocationChunk(token, session)
      if (r._2.size == readConf.fetchSize) {
        lapSw.lap(s"fbr-two-queries.locations.full", "Getting next chunk of locations (keys)")
        locationsFull.map(_.stop())
      } else {
        lapSw.lap("fbr-two-queries.locations.notFull", s"Getting next chunk of locations (keys), size = ${r._2.size}")
        locationsNotFull.map(_.stop())
      }
      logDebug(s"nextLocationChunk(token=$token) returns:\n  token: ${r._1}\n  locations: ${r._2}")

      dataBuffer.clear()

      r match {
        case (_, Nil) =>
          lapSw.lap("fbr-two-queries.empty", "Chunk of locations (keys) is empty")
          RiakConnectorSource.instance.foreach(_.fbr2EmptyChunk.mark())
          (None, Nil)
        case (nextToken: T, locations: Iterable[Location]) =>
          val valuesFull = RiakConnectorSource.instance.map(_.fbr2ValuesFull.time())
          val valuesNotFull = RiakConnectorSource.instance.map(_.fbr2ValuesNotFull.time())
          /**
           * To be 100% sure that massive fetch doesn't lead to the connection pool starvation,
           * fetch will be performed by the smaller chunks of keys.
           *
           * Ideally the chunk size should be equal to the max number of connections for the RiakNode
           */
          val itChunkedLocations = locations.grouped(RiakConnector.getMinConnectionsPerNode(session))
          fetchValues(session, itChunkedLocations, dataBuffer)

          if (readConf.fetchSize == locations.size) {
            fullChunkSw.stop(s"fbr-two-queries.full", "Entire data chunk loaded")
            lapSw.stop(s"fbr-two-queries.values.full", s"Getting full list of values (fetchSize = ${readConf.fetchSize})")
            valuesFull.map(_.stop())
            fullChunkCtx.map(_.stop())
          } else {
            fullChunkSw.stop("fbr-two-queries.notFull", s"Not full data chunk loaded ${locations.size}")
            lapSw.stop(s"fbr-two-queries.values.notFull", s"Less then ${readConf.fetchSize} keys returned")
            valuesNotFull.map(_.stop())
            notFullChunkCtx.map(_.stop())
          }
          
          (nextToken, dataBuffer.toList)
      }
    }
    catch {
      case e: Throwable =>
        fullChunkSw.stop("data-chunk.error", e)
        lapSw.stop("data-chunk.error", e)
        RiakConnectorSource.instance.foreach(_.fbr2ErrorChunk.mark())
        throw e
    }
  }

  private def fetchValues(riakSession: RiakClient, chunkedLocations: Iterator[Iterable[Location]], buffer: ArrayBuffer[(Location, RiakObject)]) ={

    while(chunkedLocations.hasNext){
      val builder = new MultiFetch.Builder()
        .withOption(FetchValue.Option.R, Quorum.oneQuorum())

      val locations = chunkedLocations.next()
      logTrace(s"Fetching ${locations.size} values...")

      locations.foreach(builder.addLocation)

      val mfr = riakSession.execute(builder.build())


      for {f <- mfr.getResponses} {

        logTrace( s"Fetch value [${buffer.size + 1}] for ${f.getQueryInfo}")

        val r = f.get()
        val location = f.getQueryInfo

        if (r.isNotFound) {
          // TODO: add proper error handling
          logWarning(s"Nothing was found for location '${f.getQueryInfo.getKeyAsString}'")
        } else if (r.hasValues) {
          if (r.getNumberOfValues > 1) {
            throw new IllegalStateException(s"Fetch for '$location' returns more than one result: ${r.getNumberOfValues} actually")
          }

          val ro = r.getValue(classOf[RiakObject])
          buffer += ((location, ro))
        } else {
          logWarning(s"There is no value for location '$location'")
        }
      }
    }
    logDebug(s"${buffer.size} values were fetched")
  }
}

object Query{
  def apply[K](bucket: BucketDef, readConf:ReadConf, queryData: QueryData[K]): Query[_] = {

    val ce = queryData.coverageEntries match {
      case None => None
      case Some(entries) =>
        require(entries.size == 1)
        Some(entries.head)
    }

    queryData.keysOrRange match {
      case Some(Left(keys: Seq[K])) =>
        if( queryData.index.isDefined){
          // Query 2i Keys
          new Query2iKeys[K](bucket, readConf, queryData.index.get, keys)
        }else{
          // Query Bucket Keys
          new QueryBucketKeys(bucket, readConf, keys.asInstanceOf[Seq[String]])
        }

      case Some(Right(range: Seq[(K, Option[K])])) =>
        // Query 2i Range, local (queryData.coverageEntries is provided) or not
        require(queryData.index.isDefined)
        require(range.size == 1)
        val r = range.head
        new Query2iKeySingleOrRange[K](bucket, readConf, queryData.index.get, r._1, r._2, ce)

      case None =>
        // Full Bucket Read
        require(queryData.index.isDefined)
        require(queryData.coverageEntries.isDefined)

        val ce = queryData.coverageEntries.get
        require(ce.nonEmpty)

        if(readConf.useStreamingValuesForFBRead){
          new QueryFullBucket(bucket, readConf, ce)
        } else {
          new Query2iKeys(bucket, readConf, queryData.index.get, ce)
        }
    }
  }
}
