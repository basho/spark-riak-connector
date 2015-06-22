package com.basho.spark.connector.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.After

abstract class AbstractRiakSparkTest extends AbstractRiakTest {
  // SparkContext, created per test case
  protected var sc:SparkContext = null

  protected def initSparkConf():SparkConf = new SparkConf(false)
    .setMaster("local")
    .setAppName(getClass.getSimpleName)
    .set("spark.riak.connection.host", DEFAULT_RIAK_HOST + ":" + DEFAULT_RIAK_PORT)
    .set("spark.riak.output.wquorum", "1")
    .set("spark.riak.input.fetch-size", "2")

  override def initialize(): Unit = {
    super.initialize()

    sc = new SparkContext(initSparkConf())
  }

  @After
  def destroySparkContext(): Unit = {
    if(sc != null){
      sc.stop()
    }
  }
}
