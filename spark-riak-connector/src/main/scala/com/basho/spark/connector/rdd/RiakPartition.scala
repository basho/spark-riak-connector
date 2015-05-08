package com.basho.spark.connector.rdd

import com.google.common.net.HostAndPort
import org.apache.spark.Partition

case class RiakPartition (index: Int,
                     endpoints: Iterable[HostAndPort],
                     rowCount: Long) extends Partition

