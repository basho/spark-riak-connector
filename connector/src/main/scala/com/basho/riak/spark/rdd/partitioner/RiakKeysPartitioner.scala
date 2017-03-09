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
package com.basho.riak.spark.rdd.partitioner

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.query.QueryData
import com.basho.riak.spark.rdd.{ RiakPartition, ReadConf }
import org.apache.spark.Partition
import java.math.BigInteger

case class RiakKeysPartition[K](
  index: Int,
  endpoints: Iterable[HostAndPort],
  keys: QueryData[K]) extends RiakPartition

object RiakKeysPartitioner {
  def partitions[K](endpoints: Iterable[HostAndPort], readConf: ReadConf, riakKeys: QueryData[K]): Array[Partition] = {
    riakKeys.keysOrRange match {
      case Some(Left(keys)) =>
        Array(new RiakKeysPartition[K](0, endpoints, riakKeys))

      case Some(Right(ranges: Seq[(K, Option[K])])) =>
        ranges match {
          case (from, to) +: Seq() => {
            val splitRanges = splitRangeIntoSubranges(from, to, readConf.getOrDefaultSplitCount())
            partitionPerRange(splitRanges, endpoints, riakKeys.index)
          }
          case _ => partitionPerRange(ranges, endpoints, riakKeys.index)
        }
    }
  }

  def partitionPerRange[K](ranges: Seq[(K, Option[K])], endpoints: Iterable[HostAndPort], index: Option[String]): Array[Partition] = {
    ranges.zipWithIndex.map {
      case (range, indx) => new RiakKeysPartition[K](indx, endpoints, new QueryData[K](Some(Right(Seq(range))), index))
    }.toArray
  }

  // TODO: move to PartitionUtils
  def calculateRanges[T: Integral](from: T, to: T, splitCount: Int)(implicit num: Integral[T]): Seq[(T, T)] = {
    import num._
    val diff = (to - from) / num.fromInt(splitCount - 1)
    val partitionsCount = if (diff == 0) 1 else splitCount
    val start = (0 to (partitionsCount - 1)).map(x => from + num.fromInt(x) * diff) 
    val end = start.tail.map(_ - num.fromInt(1)) :+ to
    start zip end
  }

  def splitRangeIntoSubranges[K](from: K, to: Option[K], splitCount: Int): Seq[(K, Option[K])] = {
    to match {
      case Some(rangeEnd: Int) => from match {
        case rangeStart: Int => {
          calculateRanges(rangeStart, rangeEnd, splitCount).map(r => (r._1.asInstanceOf[K], Some(r._2.asInstanceOf[K])))
        }
        case _ => throw new IllegalArgumentException
      }
      case Some(rangeEnd: Long) => from match {
        case rangeStart: Long => {
          calculateRanges(rangeStart, rangeEnd, splitCount).map(r => (r._1.asInstanceOf[K], Some(r._2.asInstanceOf[K])))
        }
        case _ => throw new IllegalArgumentException
      }
      case Some(rangeEnd: BigInt) => from match {
        case rangeStart: BigInt => {
          calculateRanges(rangeStart, rangeEnd, splitCount).map(r => (r._1.asInstanceOf[K], Some(r._2.asInstanceOf[K])))
        }
        case _ => throw new IllegalArgumentException
      }
      case _ => Seq((from, to))
    }
  }
}
