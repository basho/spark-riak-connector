package com.basho.riak.spark.examples.streaming

import java.util.UUID

import kafka.serializer.StringDecoder
import com.basho.riak.spark._
import com.basho.riak.spark.streaming._
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple demo for Spark streaming job integration.
  * For correct execution:
  *   kafka broker must be installed and running;
  *   'streaming' topic must be created in kafka;
  *   riak kv, kafka and spark master hostnames should be provided as spark configs (or local versions will be used).
  **/
object StreamingKVExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf(true)
      .setAppName("Simple Spark Streaming to Riak KV Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    setSparkOpt(sparkConf, "kafka.broker", "127.0.0.1:9092")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val streamCtx = new StreamingContext(sc, Durations.seconds(15))

    val kafkaProps = Map[String, String](
      "metadata.broker.list" -> sparkConf.get("kafka.broker"),
      "client.id" -> UUID.randomUUID().toString
    )

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamCtx, kafkaProps, Set[String]("ingest-kv")
    ) map { case (key, value) =>
      val obj = RiakObjectConversionUtil.to(value)
      obj.setContentType("application/json")
      obj
    } saveToRiak "test-data"

    streamCtx.start()
    println("Spark streaming context started. Spark UI could be found at http://SPARK_MASTER_HOST:4040")
    println("NOTE: if you're running job on the 'local' master open http://localhost:4040")
    streamCtx.awaitTermination()
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
