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
import com.basho.spark.connector.rdd.{ReadConf, BucketDef}

/**
 * Generic Riak Query
 */
trait Query[T] extends Serializable {
  def bucket: BucketDef
  def readConf: ReadConf

  def nextLocationBulk(nextToken: Option[_], session: RiakClient): (Option[T], Iterable[Location])
}

object Query{
  def apply[K](bucket: BucketDef, readConf:ReadConf, riakKeys: RiakKeys[K]): Query[K] = {
    riakKeys.keysOrRange match {
      case Left(keys: Seq[K]) =>
        if( riakKeys.index.isDefined){
          // Query 2i Keys
          new Query2iKeys[K](bucket, readConf, riakKeys.index.get, keys).asInstanceOf[Query[K]]
        }else{
          new QueryBucketKeys(bucket, readConf, keys.asInstanceOf[Seq[String]] ).asInstanceOf[Query[K]]
        }

      case Right(range: Seq[(K, Option[K])]) =>
        require(riakKeys.index.isDefined)
        require(range.size == 1)
        val r = range.head
        new Query2iKeySingleOrRange[K](bucket, readConf, riakKeys.index.get, r._1, r._2).asInstanceOf[Query[K]]
    }
  }
}
