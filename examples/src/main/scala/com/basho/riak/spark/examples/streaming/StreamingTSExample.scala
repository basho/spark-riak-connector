package com.basho.riak.spark.examples.streaming

import java.util.UUID

import kafka.serializer.StringDecoder
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import com.basho.riak.spark.streaming._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Example shows how Spark Streaming can be used with Riak TS Dataframes
  * For correct execution:
  *   kafka broker must be installed and running;
  *   'streaming' topic must be created in kafka;
  *   riak ts, kafka and spark master hostnames should be provided as spark configs (or local versions will be used);
  *   riak ts table should be created and activated:
  *
  * CREATE TABLE ts_weather_demo
  *  (
  *     weather      varchar not null,
  *     family       varchar not null,
  *     time         timestamp not null,
  *     temperature  double,
  *     humidity     double,
  *     pressure     double,
  *     PRIMARY KEY (
  *         (weather, family, quantum(time, 1, 'h')), weather, family, time
  *     )
  *  )
  **/
object StreamingTSExample {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf(true)
      .setAppName("Simple Spark Streaming to Riak TS Demo")

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

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamCtx, kafkaProps, Set[String]("ingest-ts")
    ) map { case (key, value) =>
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val wr = mapper.readValue(value, classOf[Map[String,String]])
      Row(
        wr("weather"),
        wr("family"),
        DateTime.parse(wr("time"),DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")).getMillis,
        wr("temperature"),
        wr("humidity"),
        wr("pressure"))
    } saveToRiakTS "ts_weather_demo"

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
