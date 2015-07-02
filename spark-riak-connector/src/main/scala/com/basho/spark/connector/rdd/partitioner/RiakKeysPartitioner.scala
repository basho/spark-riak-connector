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
package com.basho.spark.connector.rdd.partitioner

import com.basho.spark.connector.query.RiakKeys
import com.basho.spark.connector.rdd.RiakPartition
import com.google.common.net.HostAndPort
import org.apache.spark.Partition

case class RiakKeysPartition[K] (
    index: Int,
    endpoints: Iterable[HostAndPort],
    keys: RiakKeys[K]
  ) extends RiakPartition

object RiakKeysPartitioner{
  def partitions[K](endpoints: Iterable[HostAndPort], riakKeys: RiakKeys[K]): Array[Partition] = {
    riakKeys.keysOrRange match {
      case Left(keys) =>
        Array( new RiakKeysPartition(0, endpoints, riakKeys))

      case Right(ranges: Seq[(K, Option[K])]) =>
        var partitionIdx = -1
        val partitions = for {
          range <- ranges
          partition = new RiakKeysPartition({partitionIdx += 1; partitionIdx }, endpoints, new RiakKeys[K](Right(Seq(range)), riakKeys.index) )
        } yield partition

        partitions.toArray
    }
  }
}
