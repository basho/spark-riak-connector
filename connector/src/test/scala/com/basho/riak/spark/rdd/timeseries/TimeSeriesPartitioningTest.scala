/**
 * *****************************************************************************
 * Copyright (c) 2016 IBM Corp.
 *
 * Created by Basho Technologies for IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * *****************************************************************************
 */
package com.basho.riak.spark.rdd.timeseries

import java.sql.Timestamp
import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.junit.After
import org.junit.Test
import com.basho.riak.spark.rdd.partitioner.RiakTSPartition
import junit.framework.Assert._
import org.junit.rules.ExpectedException
import org.junit.Rule
import scala.annotation.meta.getter

class TimeSeriesPartitioningTest {

  val _expectedException: ExpectedException = ExpectedException.none

  @Rule
  def expectedException: ExpectedException = _expectedException

  val schema = StructType(List(
    StructField(name = "time", dataType = TimestampType),
    StructField(name = "user_id", dataType = StringType),
    StructField(name = "temperature_k", dataType = DoubleType)))

  val bucketName = "bucket"
  val partitionsCount = 15
  val fromMillis: Long = 10000
  val toMillis: Long = fromMillis + partitionsCount
  val from: Timestamp = new Timestamp(fromMillis)
  val to: Timestamp = new Timestamp(toMillis)
  val tsRangeFieldName = "time"

  val sparkConf = new SparkConf().setAppName("Riak TS Spark Dataframes Example").setMaster("local")

  val sc = new SparkContext(sparkConf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  @After
  def destroySparkContext(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  @Test
  def withOptionTest(): Unit = {
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    val range = partitions.map(x => x.asInstanceOf[RiakTSPartition].queryData.values.map(y => y.asInstanceOf[Long])).map { x => x(1) - x(0) }.sum
    assertEquals(toMillis - fromMillis + 1, range)
    assertEquals(partitionsCount, partitions.size)
  }

  @Test
  def witouthOptionTest(): Unit = {
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(1, partitions.size)
  }

  @Test
  def greaterThanToLessThanOrEqualTest(): Unit = {
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    val start = partitions.head.asInstanceOf[RiakTSPartition].queryData.values.head.asInstanceOf[Long]
    val end = partitions.last.asInstanceOf[RiakTSPartition].queryData.values.last.asInstanceOf[Long]
    assertEquals(fromMillis + 1, start)
    assertEquals(toMillis + 1, end)
    assertEquals(partitionsCount, partitions.size)
  }

  @Test
  def lessThanToGreaterThanOrEqualTest(): Unit = {
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time < CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    val start = partitions.head.asInstanceOf[RiakTSPartition].queryData.values.head.asInstanceOf[Long]
    val end = partitions.last.asInstanceOf[RiakTSPartition].queryData.values.last.asInstanceOf[Long]
    assertEquals(fromMillis, start)
    assertEquals(toMillis, end)
    assertEquals(partitionsCount, partitions.size)
  }

  @Test
  def noLessThanTest(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(s"No LessThanOrEqual or LessThan filers found for tsRangeFieldName $tsRangeFieldName")
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP)")
    val partitions = df.rdd.partitions
  }

  @Test
  def noGreaterThanTest(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(s"No GreaterThanOrEqual or GreaterThan filers found for tsRangeFieldName $tsRangeFieldName")
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time <=  CAST('$to' AS TIMESTAMP)")
    val partitions = df.rdd.partitions
  }
  
  @Test
  def noFiltersForFieldTest(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(s"No filers found for tsRangeFieldName $tsRangeFieldName")
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"user_id = 'user1'")
    val partitions = df.rdd.partitions
  }
}