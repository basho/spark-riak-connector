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
