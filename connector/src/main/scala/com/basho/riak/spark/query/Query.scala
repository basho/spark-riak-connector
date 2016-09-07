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
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}
import org.apache.spark.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Generic Riak Query
  */
trait Query[T] extends Serializable with Logging {
  type ResultT = (Location, RiakObject)

  def bucket: BucketDef

  def readConf: ReadConf

  def riakConnector: RiakConnector

  def nextChunk(nextToken: Option[T]): (Option[T], Iterable[ResultT])
}

trait LocationQuery[T] extends Query[T] {

  protected def nextLocationChunk(nextToken: Option[T]): (Option[T], Iterable[Location])

  def nextChunk(nextToken: Option[T]): (Option[T], Iterable[ResultT]) = nextLocationChunk(nextToken) match {
    case (token, locations) => token -> fetchValues(locations)
  }

  protected def fetchValues(locations: Iterable[Location], host: Option[HostAndPort] = None): ArrayBuffer[ResultT] = {
    val dataBuffer: ArrayBuffer[ResultT] = new ArrayBuffer[ResultT](readConf.fetchSize)

    riakConnector.withSessionDo(host.map(Seq(_))) { session =>
      /*
       * To be 100% sure that massive fetch doesn't lead to the connection pool starvation,
       * fetch will be performed by the smaller chunks of keys.
       *
       * Ideally the chunk size should be equal to the max number of connections for the RiakNode
       */
      val groupedLocations = locations.grouped(session.minConnectionsPerNode)
      groupedLocations foreach { locations =>
        logTrace(s"Fetching ${locations.size} values...")

        val builder = new MultiFetch.Builder()
          .withOption(FetchValue.Option.R, Quorum.oneQuorum())
          .addLocations(locations.toSeq)

        session.execute(builder.build()) foreach { future =>
          logTrace(s"Fetch value [${dataBuffer.size + 1}] for ${future.getQueryInfo}")

          val location = future.getQueryInfo
          future.get() match {
            case r: FetchValue.Response if r.isNotFound =>
              logWarning(s"Nothing was found for location '${future.getQueryInfo.getKeyAsString}'") // TODO: add proper error handling
            case r: FetchValue.Response if r.hasValues && r.getNumberOfValues > 1 =>
              throw new IllegalStateException(s"Fetch for '$location' returns more than one result: ${r.getNumberOfValues} actually")
            case r: FetchValue.Response =>
              dataBuffer += location -> r.getValue(classOf[RiakObject])
            case _ => logWarning(s"There is no value for location '$location'")
          }
        }
      }
    }
    logDebug(s"${dataBuffer.size} values were fetched")
    dataBuffer
  }
}

trait DirectLocationQuery[T] extends LocationQuery[Either[T, CoverageEntry]] {

  protected var coverageEntry: Option[CoverageEntry] = None

  protected val coverageEntriesIt: Option[Iterator[CoverageEntry]]

  protected def primaryHost = coverageEntry.map(e => HostAndPort.fromParts(e.getHost, e.getPort))

  protected def nextCoverageEntry(nextToken: Option[Either[String, CoverageEntry]]): Option[CoverageEntry] = {
    nextToken match {
      case None => coverageEntriesIt.map(_.next) // First call, next token is not defined yet
      case Some(Left(_)) => coverageEntry // Recursive call occurred. We still querying same CE
      case Some(Right(ce: CoverageEntry)) => Some(ce) // Next CE was provided
      case _ => throw new IllegalArgumentException("Invalid nextToken")
    }
  }

  /* We must override this method to ensure data locality and be 100% sure that
     values and locations were queried from the same host. This behaviour achieves by
     fetching locations and it's values from current coverage entry's host */
  override protected def fetchValues(locations: Iterable[Location],
                                     host: Option[HostAndPort]
                                    ): ArrayBuffer[(Location, RiakObject)] = host match {
    case Some(_) => super.fetchValues(locations, host)
    case None => super.fetchValues(locations, primaryHost) // use coverage entry's host for querying values
  }
}

object Query {
  def apply[K](bucket: BucketDef, readConf: ReadConf, connector: RiakConnector, queryData: QueryData[K]): Query[_] = {

    val ce = queryData.coverageEntries

    queryData.keysOrRange match {
      case Some(Left(keys: Seq[K])) =>
        if (queryData.index.isDefined) {
          // Query 2i Keys
          new Query2iKeys[K](bucket, readConf, connector, queryData.index.get, keys)
        } else {
          // Query Bucket Keys
          new QueryBucketKeys(bucket, readConf, connector, keys.asInstanceOf[Seq[String]])
        }

      case Some(Right(range: Seq[(K, Option[K])])) =>
        // Query 2i Range, local (queryData.coverageEntries is provided) or not
        require(queryData.index.isDefined)
        require(range.size == 1)
        val r = range.head
        new Query2iKeySingleOrRange[K](bucket, readConf, connector, queryData.index.get, r._1, r._2, ce)

      case None =>
        // Full Bucket Read
        require(queryData.index.isDefined)
        require(ce.isDefined)
        require(ce.get.nonEmpty)

        new QueryFullBucket(bucket, readConf, connector, ce.get, queryData.index)
    }
  }
}
