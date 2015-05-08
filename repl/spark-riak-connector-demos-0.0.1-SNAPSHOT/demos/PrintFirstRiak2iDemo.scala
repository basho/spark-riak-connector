package com.basho.spark.connector.demos

import org.apache.spark.{SparkContext, SparkConf}
import com.basho.spark.connector._
import org.apache.spark.SparkContext._

case class TSData(latitude: Float, longitude: Float, timestamp: String, user_id: String)

object PrintFirstRiak2iDemo {
  val DEFAULT_INDEX_NAME = "creationNo"
  val DEFAULT_BUCKET_NAME = "test-bucket"
  val DEFAULT_FROM = 1
  val DEFAULT_TO = 4

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Print 1st element Riak 2i")


    val indexName = sparkConf.get("spark.riak.demo.index", DEFAULT_INDEX_NAME)
    val bucketName = sparkConf.get("spark.riak.demo.bucket", DEFAULT_BUCKET_NAME)
    val from = sparkConf.get("spark.riak.demo.from", DEFAULT_FROM.toString)
    val to = sparkConf.get("spark.riak.demo.to", DEFAULT_TO.toString)

    val sc = new SparkContext(sparkConf)
    val results = execute(sc, from.toLong, to.toLong, indexName, bucketName)
    println("Results are:")
    results.foreach(println)
    println()
  }

  def execute(sc: SparkContext, from: Long = DEFAULT_FROM, to: Long = DEFAULT_TO, index: String = DEFAULT_INDEX_NAME, bucket: String = DEFAULT_BUCKET_NAME ) = {
    println("\n\nSimple Spark 2i demo(ReduceByKey):\n" +
      s"\tbucket: $bucket\n" +
      s"\tindex: $index\n" +
      s"\trange: $from:$to")

    val rdd = sc.riakBucket[TSData](bucket)
      .query2iRange(index, from.toLong, to.toLong)

    val totalCount = rdd.count()
    val pairs = rdd.map(x => (x.user_id, 1))

    val reduced = pairs.reduceByKey((a, b) => a + b)
      .sortBy(_._2)
      .collect()

    println(s"\n$totalCount values were read and reduced to ${reduced.length}")
    reduced
  }
}
