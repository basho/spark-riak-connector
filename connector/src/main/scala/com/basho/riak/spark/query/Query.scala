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
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{RiakObject, Location}
import com.basho.riak.spark.rdd.{RiakConnector, ReadConf, BucketDef}
import org.perf4j.log4j.Log4JStopWatch
import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging

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
    val entireTimeSw = new Log4JStopWatch()
    val sw = new Log4JStopWatch()
    try {
      val r = nextLocationChunk(token, session)
      if (r._2.size == readConf.fetchSize) {
        sw.lap(s"data-chunk.locations.${readConf.fetchSize}", "Getting next chunk of locations (keys)")
      } else {
        sw.lap("data-chunk.locations.notFull", s"Getting next chunk of locations (keys), size = ${r._2.size}")
      }
      logDebug(s"query(token=$token) returns:\n  token: ${r._1}\n  locations: ${r._2}")

      dataBuffer.clear()

      r match {
        case (_, Nil) =>

          /**
           * It is Absolutely possible situation, for instance:
           * in case when the last data page will be returned as a result of  2i continuation query and
           * this page will be fully filled with data then the valid continuation token wile be also returned (it will be not null),
           * therefore additional/subsequent data fetch request will be required.
           * As a result of such call the empty locations list and Null continuation token will be returned
           */
          logDebug("All data was processed (location list is empty)")
          sw.lap("data-chunk.empty", "Chunk of locations (keys) is empty")
          entireTimeSw.stop()
          (None, Nil)
        case (nextToken: T, locations: Iterable[Location]) =>

          /**
           * To be 100% sure that massive fetch doesn't lead to the connection pool starvation,
           * fetch will be performed by the smaller chunks of keys.
           *
           * Ideally the chunk size should be equal to the max number of connections for the RiakNode
           */
          val itChunkedLocations = locations.grouped(RiakConnector.getMinConnectionsPerNode(session))
          fetchValues(session, itChunkedLocations, dataBuffer)

          logDebug(s"Next data buffer was fetched:\n" +
            s"\tnextToken: $nextToken\n" +
            s"\tbuffer: $dataBuffer")
          if (readConf.fetchSize == locations.size) {
            entireTimeSw.stop(s"data-chunk.${readConf.fetchSize}", "Entire data chunk loaded")
            sw.stop(s"data-chunk.values.${readConf.fetchSize}", s"Getting full list of values (fetchSize = ${readConf.fetchSize})")
          } else {
            entireTimeSw.stop("data-chunk.notFull", s"Not full data chunk loaded ${locations.size}")
            sw.stop(s"data-chunk.values.notFull", s"Less then ${readConf.fetchSize} keys returned")
          }

          (nextToken, dataBuffer.toList)
      }
    }
    catch {
      case e: Throwable =>
        entireTimeSw.stop("data-chunk.error", e)
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
        require(!ce.isEmpty)

        new Query2iKeys[CoverageEntry](bucket, readConf, queryData.index.get, ce)
    }
  }
}
