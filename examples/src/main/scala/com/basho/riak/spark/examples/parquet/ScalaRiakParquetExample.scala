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
package com.basho.riak.spark.examples.parquet
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf

/**
  * Simple demo which illustrates how data can be extracted from Riak TS and saved as a parquet file
  *
  * CREATE TABLE parquet_demo (
  *    site varchar not null,
  *    species varchar not null,
  *    measurementDate timestamp not null,
  *    latitude double,
  *    longitude double,
  *    value double,
  *    PRIMARY KEY ((site, species, quantum(measurementDate, 24, h)),
  *    site, species, measurementDate))
  */
object ScalaRiakParquetExample {
  case class TSData(site: String, species: String, measurementDate: Long, latitude: Double, longitude: Double, value: Double )
  val startDate = System.currentTimeMillis()
  val endDate = startDate + 100
  val tableName = "parquet_demo"
  val parquetFileName = "riak-ts-data.parquet"

  val testData = Seq(
    TSData("MY7", "PM10", startDate, 51.52254, -0.15459, 41.4),
    TSData("MY7", "PM10", startDate + 10, 51.52254, -0.15459, 41.2),
    TSData("MY7", "PM10", startDate + 20, 51.52254, -0.15459, 39.1),
    TSData("MY7", "PM10", startDate + 30, 51.52254, -0.15459, 39.5),
    TSData("MY7", "PM10", startDate + 40, 51.52254, -0.15459, 29.9),
    TSData("MY7", "PM10", startDate + 50, 51.52254, -0.15459, 34.2),
    TSData("MY7", "PM10", startDate + 60, 51.52254, -0.15459, 28.5),
    TSData("MY7", "PM10", startDate + 70, 51.52254, -0.15459, 39.6),
    TSData("MY7", "PM10", startDate + 80, 51.52254, -0.15459, 29.2),
    TSData("MY7", "PM10", startDate + 90, 51.52254, -0.15459, 31.3)
  )

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Simple Scala Riak TS Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    println(s"Test data start time: $startDate")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext


    import sparkSession.implicits._

    val rdd = sc.parallelize(testData)
    rdd.toDF().write.format("org.apache.spark.sql.riak")
      .mode(SaveMode.Append).save(tableName)

    val df = sparkSession.read.format("org.apache.spark.sql.riak")
      .load(tableName).createOrReplaceTempView(tableName)

    val from = (startDate / 1000).toInt
    val query = s"select * from $tableName where measurementDate >= CAST($from AS TIMESTAMP) " +
      s"AND measurementDate <= CAST(${from + 1} AS TIMESTAMP) AND site = 'MY7' AND species = 'PM10'"

    println(s"Query: $query")
    val rows = sparkSession.sql(query)
    rows.show()
    val schema = rows.schema

    rows.write.mode("overwrite").parquet(parquetFileName)
    println(s"Data was successfully saved to Parquet file: $parquetFileName")

    val parquetFile = sparkSession.read.parquet(parquetFileName)
    parquetFile.createTempView("parquetFile")
    val data = sparkSession.sql("SELECT MAX(value) max_value FROM parquetFile ")

    println("Maximum value retrieved from Parquet file:")
    data.show()
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
