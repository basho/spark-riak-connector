package com.basho.spark.connector.demos

import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.rdd.RiakFunctions
import org.apache.spark.{SparkContext, SparkConf}
import com.basho.spark.connector._

/**
 * Really simple demo program which calculates the number of records loaded
 * from the Riak using 2i range query
 */
object SimpleScalaRiak2iDemo {
  private val SOURCE_DATA = new Namespace("test-data")

  private val TEST_DATA: String =
    "[" +
        "  {key: 'key-1', indexes: {creationNo: 1}, value: 'value1'}" +
        ", {key: 'key-2', indexes: {creationNo: 2}, value: 'value2'}" +
        ", {key: 'key-3', indexes: {creationNo: 3}, value: 'value3'}" +
        ", {key: 'key-4', indexes: {creationNo: 4}, value: 'value4'}" +
        ", {key: 'key-5', indexes: {creationNo: 5}, value: 'value5'}" +
        ", {key: 'key-6', indexes: {creationNo: 6}, value: 'value6'}" +
    "]"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Simple Scala Riak Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    //setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:10017")


    createTestData(sparkConf)
      val sc = new SparkContext(sparkConf)
      val rdd = sc.riakBucket(SOURCE_DATA)
        .query2iRange("creationNo", 0L, 1000L)

      println(s"Execution result: ${rdd.count()}")
  }

  private def createTestData(sparkConf: SparkConf): Unit = {
    val rf = RiakFunctions(sparkConf)

    rf.withRiakDo(session => {
      rf.createValues(session, SOURCE_DATA, TEST_DATA, true )
    })
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
