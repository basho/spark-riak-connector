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

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.Location
import org.apache.spark.Logging

private trait QuerySubsetOfKeys[K] extends Query[Int] with Logging{
  def keys: Iterable[K]
  private var _iterator: Option[Iterator[K]] = None
  private var _nextPos: Int = -1

  def locationsByKeys(keys: Iterator[K], session: RiakClient): (Iterable[Location])

  // scalastyle:off cyclomatic.complexity
  final override def nextLocationBulk(nextToken: Option[_], session: RiakClient): (Option[Int], Iterable[Location]) = {
    nextToken match {
      case None | Some(0) =>
        // it is either the first call or a kind of "random" read request of reading the first bulk
        _iterator = Some(keys.iterator) // grouped readConf.fetchSize
        _nextPos = 0

      case Some(requested: Int) if requested == _nextPos =>
      // subsequent read request, there is nothing to do

      case Some(requested: Int) if requested != 0 && requested != _nextPos =>
        // random read request, _iterator should be adjusted
        logWarning(s"nextLocationBulk: random read was requested, it may cause performance issue:\n" +
          s"\texpected position: ${_nextPos}, while the requested read position is $requested")
        _nextPos = requested -1
        _iterator = Some(keys.iterator.drop(_nextPos)) // grouped(readConf.fetchSize)

      case _ =>
        throw new IllegalArgumentException("Wrong nextToken")
    }

    assert(_iterator.isDefined)

    if(!_iterator.get.hasNext){
      // TODO: Add proper error handling
      throw new IllegalStateException()
    }

    val locations = locationsByKeys(_iterator.get, session)
    _nextPos += locations.size

    ( _iterator.get.hasNext match {
      case true => Some(_nextPos)
      case _ => None
    }, locations)
  }
  // scalastyle:off cyclomatic.complexity
}
