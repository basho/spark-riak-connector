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

import org.apache.spark.Logging

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.{RiakObject, Location}

class DataQueryingIterator(query: Query[_], riakSession: RiakClient, minConnectionsPerNode: Int)
  extends Iterator[(Location, RiakObject)] with Logging {

  type ResultT = (Location, RiakObject)

  private var isThereNextValue: Option[Boolean] = None
  private var nextToken: Option[_] = None

  private var _iterator: Option[Iterator[ResultT]] = None

  protected[this] def prefetchIfNeeded(): Boolean = {
    // scalastyle:off return
    if (this._iterator.isDefined) {
      if(this._iterator.get.hasNext){
        // Nothing to do, we still have at least one result
        logTrace(s"prefetch is not required, at least one value was pre-fetched")
        return false
      }else if(nextToken.isEmpty){
        // Nothing to do, we already got all the data
        logTrace("prefetch is not required, all data has been processed")
        return false
      }
    }
    // scalastyle:on return

    logTrace(s"Performing query(token=$nextToken)")

    val r = query.nextChunk(nextToken, riakSession )
    logDebug(s"query(token=$nextToken) returns:\n  token: ${r._1}\n  data: ${r._2}")
    nextToken = r._1

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
      case (_, data: Iterable[(Location,RiakObject)]) =>
        _iterator = Some(data.iterator)
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

    isThereNextValue = None
    _iterator.get.next()
  }
}
