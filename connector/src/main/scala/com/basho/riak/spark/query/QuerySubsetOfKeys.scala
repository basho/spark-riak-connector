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

import com.basho.riak.client.core.query.Location
import com.basho.riak.spark.util.CountingIterator

/**
 *
 * @tparam K may represents Bucket keys, 2i keys or Coverage entries
 */
 trait QuerySubsetOfKeys[K] extends LocationQuery[Int] {
  def keys: Iterable[K]
  private var _iterator: Option[Iterator[K]] = None
  private var _nextPos: Int = -1

  /**
   * @return _1 true, in case when additional values for the last key are available. Additional means that values
   *         are not fit into the current chunk due to the chunk size limit.
   */
  protected def locationsByKeys(keys: Iterator[K]): (Boolean, Iterable[Location])

  // scalastyle:off cyclomatic.complexity
  final override def nextLocationChunk(nextToken: Option[Int]): (Option[Int], Iterable[Location]) = {
    nextToken match {
      case None | Some(0) =>
        // it is either the first call or a kind of "random" read request of reading the first bulk
        _iterator = Some(CountingIterator(keys.iterator))
        _nextPos = 0

      case Some(requested: Int) if requested == _nextPos =>
      // subsequent read request, there is nothing to do

      case Some(requested: Int) if requested != 0 && requested != _nextPos =>
        // random read request, _iterator should be adjusted
        logWarning(s"nextLocationChunk: RANDOM READ WAS REQUESTED, it may cause performance issue:\n" +
          s"\texpected position: ${_nextPos}, while the requested read position is $requested")
        _nextPos = requested -1
        _iterator = Some(CountingIterator(keys.iterator.drop(_nextPos)))

      case _ =>
        throw new IllegalArgumentException("Wrong nextToken")
    }

    assert(_iterator.isDefined)

    val iterator: CountingIterator[K] = _iterator.get.asInstanceOf[CountingIterator[K]]
    val (hasContinuation,locations) = locationsByKeys(iterator)

    /**
     * Since there is no 1 to 1 relation between keys and locations, multiple locations might be returned for the one key
     *
     * For example in case when the same 2i key is used to index multiple values
     */
    _nextPos = iterator.count

    ( hasContinuation || iterator.hasNext match {
      case true => Some(_nextPos)
      case _ => None
    }, locations)
  }
  // scalastyle:off cyclomatic.complexity
}
