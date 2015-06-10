package com.basho.spark.connector.rdd

import com.google.common.net.HostAndPort
import org.apache.spark.Partition

trait RiakPartition extends Partition{
  def endpoints: Iterable[HostAndPort]
}

