package com.basho.riak.spark.examples.streaming

import java.util.UUID

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._

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

    val schema = StructType(List(
      StructField(name = "weather", dataType = StringType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = TimestampType),
      StructField(name = "temperature", dataType = DoubleType),
      StructField(name = "humidity", dataType = DoubleType),
      StructField(name = "pressure", dataType = DoubleType)))

    val sparkConf = new SparkConf(true)
      .setAppName("Simple Spark Streaming to Riak TS Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    setSparkOpt(sparkConf, "kafka.broker", "127.0.0.1:9092")

    val sc = new SparkContext(sparkConf)
    val streamCtx = new StreamingContext(sc, Durations.seconds(15))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val kafkaProps = Map[String, String](
      "metadata.broker.list" -> sparkConf.get("kafka.broker"),
      "client.id" -> UUID.randomUUID().toString
    )
    KafkaUtils
      .createDirectStream[String, String, StringDecoder, StringDecoder](streamCtx, kafkaProps,
      Set[String]("streaming"))
      .foreachRDD { rdd => rdd.map(println)
        val rows = sqlContext.read.schema(schema).json(rdd.values)
          .withColumn("time", 'time.cast("Timestamp"))
          .select("weather", "family", "time", "temperature", "humidity", "pressure")

        rows.write
          .format("org.apache.spark.sql.riak")
          .mode(SaveMode.Append)
          .save("ts_weather_demo")
      }

    streamCtx.start()
    streamCtx.awaitTermination()
    println("Spark streaming context started. Spark UeI could be found at http://SPARK_MASTER_HOST:4040")
    println("NOTE: if you're running job on the 'local' master open http://localhost:4040")
  }


  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
