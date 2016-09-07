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

import com.basho.riak.client.core.query.{Location, RiakObject}
import org.apache.spark.Logging

class KVDataQueryingIterator[T](query: Query[T]) extends Iterator[(Location, RiakObject)] with Logging {

  type ResultT = (Location, RiakObject)

  private var isThereNextValue: Option[Boolean] = None
  private var nextToken: Option[T] = None

  private var _iterator: Option[Iterator[ResultT]] = None

  protected[this] def prefetch(): Boolean = {
    logTrace(s"Prefetching chunk of data: query(token=$nextToken)")

    val r = query.nextChunk(nextToken)

    if( isTraceEnabled() ) {
      logTrace(s"query(token=$nextToken) returns:\n  token: ${r._1}\n  data:\n\t ${r._2}")
    } else {
      logDebug(s"query(token=$nextToken) returns:\n  token: ${r._1}\n  data.size: ${r._2.size}")
    }

    nextToken = r._1

    r match {
      case (_, Nil) =>
        /**
         * It is Absolutely possible situation, for instance:
         *     in case when the last data page will be returned as a result of  2i continuation query and
         *     this page will be fully filled with data then the valid continuation token wile be also returned (it will be not null),
         *     therefore additional/subsequent data fetch request will be required.
         *     As a result of such call the empty chunk and Null continuation token will be returned
         */
        logDebug("prefetch returned Nothing, all data was already processed (empty chunk was returned)")
        _iterator = KVDataQueryingIterator.OPTION_EMPTY_ITERATOR

      case (_, data: Iterable[(Location,RiakObject)]) =>
        if(nextToken.isEmpty){
          logDebug("prefetch returned the last chunk, all data was processed")
        }

        _iterator = Some(data.iterator)
    }

    _iterator.get.hasNext
  }

  override def hasNext: Boolean = {
    isThereNextValue match {
      case Some(b: Boolean) =>
        // cached value will be returned

      case None if _iterator.isDefined && _iterator.get.hasNext =>
        logTrace(s"prefetch is not required, at least one pre-fetched value available")
        isThereNextValue = KVDataQueryingIterator.OPTION_TRUE

      case None if _iterator.isDefined && _iterator.get.isEmpty && nextToken.isEmpty =>
        logTrace("prefetch is not required, all data was already processed")
        isThereNextValue = KVDataQueryingIterator.OPTION_FALSE

      case None =>
        isThereNextValue = Some(prefetch())
    }

    isThereNextValue.get
  }

  override def next(): (Location, RiakObject) = {
    if( !hasNext ){
      throw new NoSuchElementException("next on iterator")
    }

    isThereNextValue = None
    _iterator.get.next()
  }
}

object  KVDataQueryingIterator {
  private val OPTION_EMPTY_ITERATOR = Some(Iterator.empty)
  private val OPTION_TRUE = Some(true)
  private val OPTION_FALSE = Some(false)

  def apply[T](query: Query[T]): KVDataQueryingIterator[T] = new KVDataQueryingIterator[T](query)
}
