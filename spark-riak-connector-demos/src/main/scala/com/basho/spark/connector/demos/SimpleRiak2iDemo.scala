package com.basho.spark.connector.demos

import org.apache.spark.{SparkContext, SparkConf}
import com.basho.spark.connector._

/**
 * A really simple demo program which calculates number of records loaded
 * from the RiakRDD by using 2i range query
 */
object PrintCountRiak2iDemo {
  def main(args: Array[String]) {

      val sparkConf = new SparkConf()
        .setAppName("Count number of values Riak 2i")
      val indexName = sparkConf.get("spark.riak.demo.index", "creationIndex")
      val bucketName = sparkConf.get("spark.riak.demo.bucket", "test-bucket")
      val from = sparkConf.get("spark.riak.demo.from", "1")
      val to = sparkConf.get("spark.riak.demo.to", "4")

      println("\n\nSimple Spark 2i demo:\n" +
        s"\tbucket: $bucketName\n" +
        s"\tindex: $indexName\n" +
        s"\trange: $from:$to")

      val sc = new SparkContext(sparkConf)
      val rdd = sc.riakBucket(bucketName)
        .query2iRange(indexName, from.toLong, to.toLong)

      val count = rdd.count()
      println(s"Execution result: ${count}")
  }
}
