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
import com.basho.riak.client.core.query.Location
import com.basho.riak.spark.rdd.{ReadConf, BucketDef}

import scala.collection.mutable.ArrayBuffer

private case class Query2iKeys[K](bucket: BucketDef, readConf:ReadConf, index: String, keys: Iterable[K]) extends QuerySubsetOfKeys[K] {
  private var query2iKey: Option[Query2iKeySingleOrRange[K]] = None
  private var tokenNext: Option[String] = None

  // By default there should be an empty Serializable Iterator
  private var _iterator: Iterator[Location] = ArrayBuffer.empty[Location].iterator

  private def chunkIsCollected(chunk: Iterable[Location]) = chunk.size >= readConf.fetchSize

  override def locationsByKeys(keys: Iterator[K], session: RiakClient): (Boolean, Iterable[Location]) = {
    val dataBuffer = new ArrayBuffer[Location](readConf.fetchSize)

    while ((keys.hasNext || _iterator.hasNext || tokenNext.isDefined) && !chunkIsCollected(dataBuffer)){
      // Previously gathered results should be returned at first, if any
      _iterator forall  ( location => {
        dataBuffer += location
        !chunkIsCollected(dataBuffer)
      })

      if(!chunkIsCollected(dataBuffer)) tokenNext match {
        case Some(next) =>
          // Fetch the next results page from the previously executed 2i query, if any
          assert(query2iKey.isDefined)

          val r = query2iKey.get.nextLocationChunk( tokenNext, session)
          tokenNext = r._1
          _iterator = r._2.iterator

        case None if keys.hasNext =>
          // query data for the first/next key
          assert(_iterator.isEmpty && tokenNext.isEmpty)

          val key = keys.next()
          query2iKey = Some(new Query2iKeySingleOrRange[K](bucket, readConf, index, key))

          val r = query2iKey.get.nextLocationChunk(tokenNext, session)
          tokenNext = r._1
          _iterator = r._2.iterator

        case _ => // There is nothing to do
      }
    }
    tokenNext.isDefined -> dataBuffer
  }
}
