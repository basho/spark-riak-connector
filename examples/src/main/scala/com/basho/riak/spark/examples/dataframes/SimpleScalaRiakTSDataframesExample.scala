/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.examples.dataframes

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

/**
 * Example shows how Spark DataFrames can be used with Riak TS
 * * CREATE TABLE ts_weather_demo
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
 */
object SimpleScalaRiakTSDataframesExample {
  private val tableName = "ts_weather_demo"

  val testData = """{"time": "2016-01-01 08:00:01.000", "weather": "sunny", "temperature": 26.0, "humidity": 66.0, "pressure": 30.17, "family": "f"},
                    {"time": "2016-01-01 08:30:00.000", "weather": "sunny", "temperature": 25.0, "humidity": 67.0, "pressure": 30.20, "family": "f"},
                    {"time": "2016-01-01 09:00:00.000", "weather": "fair", "temperature": 24.0, "humidity": 69.0, "pressure": 29.78, "family": "f"},
                    {"time": "2016-01-01 09:30:00.000", "weather": "cloudy", "temperature": 23.0, "humidity": 69.0, "pressure": 28.80, "family": "f"},
                    {"time": "2016-01-01 10:00:00.000", "weather": "cloudy", "temperature": 20.0, "humidity": 70.0, "pressure": 30.27, "family": "f"},
                    {"time": "2016-01-01 10:30:00.000", "weather": "rain", "temperature": 20.0, "humidity": 75.0, "pressure": 30.28, "family": "f"}"""

  val schemaWithTimestamp = StructType(List(
    StructField(name = "weather", dataType = StringType),
    StructField(name = "family", dataType = StringType),
    StructField(name = "time", dataType = TimestampType),
    StructField(name = "temperature", dataType = DoubleType),
    StructField(name = "humidity", dataType = DoubleType),
    StructField(name = "pressure", dataType = DoubleType)))

  val schemaWithLong = StructType(List(
    StructField(name = "weather", dataType = StringType),
    StructField(name = "family", dataType = StringType),
    StructField(name = "time", dataType = LongType),
    StructField(name = "temperature", dataType = DoubleType),
    StructField(name = "humidity", dataType = DoubleType),
    StructField(name = "pressure", dataType = DoubleType)))

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Riak TS Spark Dataframes Example")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    // Load test data from json file
    println("---------------------------------- input data -----------------------------------")
    val inputRDD = sc.parallelize(testData.split("\n"))
    val inputDF = sparkSession.read.json(inputRDD)
      .withColumn("time", 'time.cast("Timestamp")) // Timestamp types are not inferred when reading from JSON and need to be cast
      .select("weather", "family", "time", "temperature", "humidity", "pressure") // column ordering should be the same as in schema
    inputDF.printSchema
    inputDF.show

    // Save DF to Riak TS
    inputDF.write
      .format("org.apache.spark.sql.riak")
      .mode(SaveMode.Append)
      .save(tableName)

    val timeVals = inputDF.select("time").rdd.map(_.getTimestamp(0).getTime).collect

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
    val fromMillis = timeVals.min
    val toMillis = timeVals.max
    val from = dateFormat.format(new Date(fromMillis))
    val to = dateFormat.format(new Date(toMillis))

    // Simple Riak range query with schema provided
    println("---------------------- Range query with provided schema -------------------------")
    val withSchemaProvided = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .schema(schemaWithTimestamp)
      .load(tableName)
      .filter(s"time >= CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP) AND  weather = 'sunny' AND family = 'f'")
    withSchemaProvided.printSchema
    withSchemaProvided.show

    // Simple Riak range query with schema provided and automatic timestamp to long conversion
    println("---Range query with provided schema and automatic timestamp to long conversion ---")
    val withSchemaProvidedLongTime = sparkSession.read
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(schemaWithLong)
      .load(tableName)
      .filter(s"time >= $fromMillis AND time <= $toMillis AND  weather = 'sunny' AND family = 'f'")
    withSchemaProvidedLongTime.printSchema
    withSchemaProvidedLongTime.show

    // Simple Riak range query without providing schema
    println("-------------------- Range query with inferred schema ---------------------------")
    val df = sparkSession.read
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .load(tableName)
      .filter(s"time >= CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP) AND  weather = 'sunny' AND family = 'f'")
    df.printSchema
    df.show
    
    // Simple Riak range query without providing schema and with useLong option for timestamp binding
    println("------ Range query with inferred schema and treating timestamps as Long (in milliseconds) ---------")
    val dfUseLong = sparkSession.read
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .option("spark.riakts.bindings.timestamp", "useLong") // option to treat timestamps as Longs
      .format("org.apache.spark.sql.riak")
      .load(tableName)
      .filter(s"time >= $fromMillis AND time <= $toMillis AND  weather = 'sunny' AND family = 'f'")
    dfUseLong.printSchema
    dfUseLong.show
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }

}
