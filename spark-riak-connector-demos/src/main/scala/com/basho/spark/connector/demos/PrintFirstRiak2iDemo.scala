package com.basho.spark.connector.demos

import com.basho.spark.connector.rdd.RiakFunctions
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.spark.connector._
import org.apache.spark.SparkContext._

case class TSData(latitude: Float, longitude: Float, timestamp: String, user_id: String)

object PrintFirstRiak2iDemo {
  val testData = "[" +
    "  {key: 'key-1', indexes: {creationNo: 1}, value: {timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}}" +
    ", {key: 'key-2', indexes: {creationNo: 2}, value: {timestamp: '2014-11-24T13:15:04.823Z', user_id: 'u1'}}" +
    ", {key: 'key-3', indexes: {creationNo: 3}, value: {timestamp: '2014-11-24T13:18:04', user_id: 'u1'}}" +
    ", {key: 'key-4', indexes: {creationNo: 4}, value: {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}}" +
    ", {key: 'key-5', indexes: {creationNo: 5}, value: {timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}}" +
    ", {key: 'key-6', indexes: {creationNo: 6}, value: {timestamp: '2014-11-24T13:21:04.823Z', user_id: 'u3'}}" +
    "]"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Print 1st element Riak 2i")

    val demoCfg = Demo2iConfig(sparkConf)
    val sc = new SparkContext(sparkConf)
    val results = execute(sc, demoCfg.from, demoCfg.to, demoCfg.index, demoCfg.bucket)
    println("Results are:")
    results.foreach(println)
    println()
  }

  def execute(sc: SparkContext, from: Long = Demo2iConfig.DEFAULT_FROM, to: Long = Demo2iConfig.DEFAULT_TO,
              index: String = Demo2iConfig.DEFAULT_INDEX_NAME, bucket: String = Demo2iConfig.DEFAULT_BUCKET_NAME ) = {

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

  def generateTestData(sc: SparkContext): Unit = {
    val demoConfig = Demo2iConfig(sc.getConf)
    val rf = RiakFunctions( demoConfig.riakNodeBuilder() )

    println(s"Generating test data for ${demoConfig.name}, '${demoConfig.bucket}' will be truncated")

    rf.withRiakDo( session =>{
        rf.createValues(session, demoConfig.riakNamespace, testData, true)
      })
  }
}
