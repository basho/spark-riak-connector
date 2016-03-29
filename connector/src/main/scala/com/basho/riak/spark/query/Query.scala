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

import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.kv.{FetchValue, MultiFetch}
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.rdd.connector.RiakSession
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Generic Riak Query
 */
trait Query[T] extends Serializable with Logging{
  type ResultT = (Location, RiakObject)

  def bucket: BucketDef
  def readConf: ReadConf

  def nextChunk(nextToken: Option[_], session: RiakSession): (Option[T], Iterable[ResultT])
}

trait LocationQuery[T] extends Query[T] {
  private val dataBuffer: ArrayBuffer[ResultT] = new ArrayBuffer[ResultT](readConf.fetchSize)

  protected def nextLocationChunk(nextToken: Option[_], session: RiakSession): (Option[T], Iterable[Location])

  def nextChunk(token: Option[_], session: RiakSession): (Option[T], Iterable[ResultT]) = {
    val r = nextLocationChunk(token, session)
    logDebug(s"nextLocationChunk(token=$token) returns:\n  token: ${r._1}\n  locations: ${r._2}")

    r match {
      case (_, Nil) =>
        (None, Nil)
      case (nextToken, locations) =>

        /**
          * To be 100% sure that massive fetch doesn't lead to the connection pool starvation,
          * fetch will be performed by the smaller chunks of keys.
          *
          * Ideally the chunk size should be equal to the max number of connections for the RiakNode
          */
        val itChunkedLocations = locations.grouped(session.minConnectionsPerNode)
        (nextToken, fetchValues(session, itChunkedLocations).toList)
    }
  }

  protected def fetchValues(riakSession: RiakSession,
                            chunkedLocations: Iterator[Iterable[Location]]): ArrayBuffer[ResultT] = {
    dataBuffer.clear()
    chunkedLocations.foreach { locations =>
      logTrace(s"Fetching ${locations.size} values...")

      val builder = new MultiFetch.Builder()
        .withOption(FetchValue.Option.R, Quorum.oneQuorum())
        .addLocations(locations.toSeq)

      riakSession.execute(builder.build()) foreach { future =>
        logTrace(s"Fetch value [${dataBuffer.size + 1}] for ${future.getQueryInfo}")

        val location = future.getQueryInfo
        future.get() match {
          case r: FetchValue.Response if r.isNotFound =>
            logWarning(s"Nothing was found for location '${future.getQueryInfo.getKeyAsString}'") // TODO: add proper error handling
          case r: FetchValue.Response if r.hasValues && r.getNumberOfValues > 1 =>
            throw new IllegalStateException(s"Fetch for '$location' returns more than one result: ${r.getNumberOfValues} actually")
          case r: FetchValue.Response =>
            dataBuffer += ((location, r.getValue(classOf[RiakObject])))
          case _ => logWarning(s"There is no value for location '$location'")
        }
      }
    }
    logDebug(s"${dataBuffer.size} values were fetched")
    dataBuffer
  }
}

object Query {
  def apply[K](bucket: BucketDef, readConf: ReadConf, queryData: QueryData[K]): Query[_] = {

    val ce = queryData.coverageEntries

    queryData.keysOrRange match {
      case Some(Left(keys: Seq[K])) =>
        if (queryData.index.isDefined) {
          // Query 2i Keys
          new Query2iKeys[K](bucket, readConf, queryData.index.get, keys)
        } else {
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

        new QueryFullBucket(bucket, readConf, ce, queryData.index)
    }
  }
}
