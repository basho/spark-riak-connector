package com.basho.spark.connector.writer

import org.apache.spark.SparkConf

case class WriteConf( writeQuorum: Int = WriteConf.DefaultWriteQuorum)

object WriteConf {
  val WriteQuorumProperty = "spark.riak.output.wquorum"

  val DefaultWriteQuorum = 1

  def fromSparkConf(conf: SparkConf): WriteConf = {
    WriteConf(
      writeQuorum = conf.getInt(WriteQuorumProperty, DefaultWriteQuorum)
    )
  }
}
