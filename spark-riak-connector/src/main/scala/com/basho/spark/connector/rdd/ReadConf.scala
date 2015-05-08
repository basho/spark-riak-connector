package com.basho.spark.connector.rdd

import org.apache.spark.SparkConf

/** RDD read settings
  * @param fetchSize number of keys to fetch in a single round-trip to Riak
 */
case class ReadConf (
  fetchSize: Int = ReadConf.DefaultFetchSize )

object ReadConf {
  val DefaultFetchSize = 1000

  def fromSparkConf(conf: SparkConf): ReadConf = {
    ReadConf(
      fetchSize = conf.getInt("spark.riak.input.page.row.size", DefaultFetchSize)
    )
  }
}
