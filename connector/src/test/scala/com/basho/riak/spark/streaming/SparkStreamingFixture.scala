package com.basho.riak.spark.streaming

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.{After, Before}

trait SparkStreamingFixture extends Logging {

  protected var sc: SparkContext

  protected var ssc: StreamingContext = _

  protected val batchDuration = Seconds(1)

  @Before
  def startStreamingContext(): Unit = {
    ssc = new StreamingContext(sc, batchDuration)
    logInfo("Streaming context created")
  }

  @After
  def stopStreamingContext(): Unit = {
    Option(ssc).foreach(_.stop())
    logInfo("Streaming context stopped")
  }
}
