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
package com.basho.spark.connector.query

import com.basho.riak.client.api.cap.Quorum
import org.apache.spark.Logging

import scala.collection.JavaConversions._

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.{FetchValue, MultiFetch}
import com.basho.riak.client.core.query.{RiakObject, Location}

import scala.collection.mutable.ArrayBuffer

class DataQueryingIterator(query: Query[_], riakSession: RiakClient, minConnectionsPerNode: Int)
  extends Iterator[(Location, RiakObject)] with Logging {

  type ResultT = (Location, RiakObject)

  // The following 2 variables are useful only for the debugging purposes
  private val dataBuffer: ArrayBuffer[ResultT] = new ArrayBuffer[ResultT](query.readConf.fetchSize)
  private var bufferIndex = 0

  private var isThereNextValue: Option[Boolean] = None
  private var nextToken: Option[_] = None

  private var _iterator: Option[Iterator[ResultT]] = None

  protected[this] def prefetchIfNeeded(): Boolean = {
    // scalastyle:off return
    if (this._iterator.isDefined) {
      if(this._iterator.get.hasNext){
        // Nothing to do, we still have at least one result
        logDebug(s"prefetch is not required, at least one pre-fetched value [actually ${dataBuffer.size-bufferIndex} from ${dataBuffer.size}] is in the buffer")
        return false
      }else if(nextToken.isEmpty){
        // Nothing to do, we already got all the data
        logDebug("prefetch is not required, all data has been processed")
        return false
      }
    }
    // scalastyle:on return

    logTrace(s"Performing 2i query(token=$nextToken)")

    val r = query.nextLocationBulk(nextToken, riakSession )
    logDebug(s"2i query(token=$nextToken) returns:\n  token: ${r._1}\n  locations: ${r._2}")
    nextToken = r._1

    dataBuffer.clear()
    bufferIndex = 0

    r match {
      case (_, Nil) =>
        /**
         * It is Absolutely possible situation, for instance:
         *     in case when the last data page will be returned as a result of  2i continuation query and
         *     this page will be fully filled with data then the valid continuation token wile be also returned (it will be not null),
         *     therefore additional/subsequent data fetch request will be required.
         *     As a result of such call the empty locations list and Null continuation token will be returned
         */
        logDebug("prefetch is not required, all data was processed (location list is empty)")
        _iterator = Some(Iterator.empty)
      case (_, locations: Iterable[Location]) =>
        /**
         * To be 100% sure that massive fetch doesn't lead to the connection pool starvation,
         * fetch will be performed by the smaller chunks of keys.
         *
         * Ideally the chunk size should be equal to min number of connections to the RiakNode
         */
        val itChunkedLocations = locations.grouped(minConnectionsPerNode)
        DataQueryingIterator.fetchData(riakSession, itChunkedLocations, dataBuffer)

        logDebug(s"Next data buffer was fetched:\n" +
          s"\tnextToken: $nextToken\n" +
          s"\tbuffer: $dataBuffer")

        _iterator = Some(dataBuffer.iterator)
    }
    true
  }

  override def hasNext: Boolean =
    isThereNextValue match {
      case Some(b: Boolean) =>
        b
      case None =>
        prefetchIfNeeded()
        val r= _iterator.get.hasNext
        isThereNextValue = Some(r)
        r
    }

  override def next(): (Location, RiakObject) = {
    if( !hasNext ){
      throw new NoSuchElementException("next on iterator")
    }

    bufferIndex += 1
    isThereNextValue = None
    _iterator.get.next()
  }
}

object DataQueryingIterator extends  Logging {

  private def fetchData(riakSession: RiakClient, chunkedLocations: Iterator[Iterable[Location]], buffer: ArrayBuffer[(Location, RiakObject)]) ={

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
    logDebug(s"${buffer.size} were fetched")
  }
}
