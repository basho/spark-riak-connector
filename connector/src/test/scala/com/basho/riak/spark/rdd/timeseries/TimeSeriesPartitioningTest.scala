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
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.LessThan
import org.junit.Test
import com.basho.riak.spark.rdd.partitioner.RiakTSPartition
import com.basho.riak.spark.toSparkContextFunctions
import org.junit.Assert.assertEquals

class TimeSeriesPartitioningTest extends AbstractTimeSeriesTest(createTestData = false) {

  val partitionsCount = 15
  val fromMillis: Long = 10000
  val toMillis: Long = fromMillis + partitionsCount
  val from: Timestamp = new Timestamp(fromMillis)
  val to: Timestamp = new Timestamp(toMillis)
  val tsRangeFieldName = "time"

  val filters: Array[Filter] = Array(
    GreaterThanOrEqual("time", from),
    LessThan("time", to),
    EqualTo("user_id", "user1"))

  @Test
  def withOptionTest(): Unit = {
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(partitionsCount, partitions.size)
  }

  @Test
  def smallRangeShouldBeSinglePartitionTest(): Unit = {
    val (localFrom, localTo) = (new Timestamp(500L), new Timestamp(504L))
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$localFrom' AS TIMESTAMP) AND time <= CAST('$localTo' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(1, partitions.size)
  }

  @Test
  def invalidRangeTest(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage("requirement failed: Invalid range query")
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .option("spark.riak.partitioning.ts-quantum", "10s")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time < CAST('$from' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
  }

  @Test
  def withOptionTestFromToTo(): Unit = {
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .option("spark.riak.partitioning.ts-quantum", "10s")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time <= CAST('$from' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(1, partitions.size)
  }

  @Test
  def withoutDFTest(): Unit = {
    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .select("time", "user_id", "temperature_k")
      .where(s"time >=  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")
    val partitions = rdd.partitions
    assertEquals(1, partitions.size)
  }

  @Test
  def greaterThanToLessThanOrEqualTest(): Unit = {
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .select("time", "user_id", "temperature_k")
      .filter(s"time >  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(partitionsCount, partitions.size)
    assertEquals(s"SELECT time, user_id, temperature_k FROM $bucketName  WHERE time >= 10001 AND time < 10002",
      partitions.head.asInstanceOf[RiakTSPartition].queryData.head.sql)
    assertEquals(s"SELECT time, user_id, temperature_k FROM $bucketName  WHERE time >= 10015 AND time < 10016",
      partitions.last.asInstanceOf[RiakTSPartition].queryData.head.sql)
  }

  @Test
  def lessThanToGreaterThanOrEqualTest(): Unit = {
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .select("time", "user_id", "temperature_k")
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time < CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(partitionsCount, partitions.size)
    assertEquals(s"SELECT time, user_id, temperature_k FROM $bucketName  WHERE time >= 10000 AND time < 10001",
      partitions.head.asInstanceOf[RiakTSPartition].queryData.head.sql)
    assertEquals(s"SELECT time, user_id, temperature_k FROM $bucketName  WHERE time >= 10014 AND time < 10015",
      partitions.last.asInstanceOf[RiakTSPartition].queryData.head.sql)
  }

  @Test
  def noLessThanTest(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(s"No LessThanOrEqual or LessThan filers found for tsRangeFieldName $tsRangeFieldName")
    val df = sparkSession.read
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
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time <=  CAST('$to' AS TIMESTAMP)")
    val partitions = df.rdd.partitions
  }

  @Test
  def withLessThanQuantaLimitTest(): Unit = {
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .option("spark.riak.partitioning.ts-quantum", "20d")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(partitionsCount, partitions.size)
    val filteredPartitions = partitions.filter(x => x.asInstanceOf[RiakTSPartition].queryData.size != 1)
    assertEquals(0, filteredPartitions.size)
  }

  @Test
  def withGreaterThanQuantaLimitTest(): Unit = {
    val (localFrom, localTo) = (new Timestamp(1000000L), new Timestamp(3000000L))
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .option("spark.riak.partitioning.ts-quantum", "10s")
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >=  CAST('$localFrom' AS TIMESTAMP) AND time < CAST('$localTo' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(partitionsCount, partitions.size)
    val filteredPartitions = partitions.filter(x => x.asInstanceOf[RiakTSPartition].queryData.size <= 1)
    assertEquals(0, filteredPartitions.size)
  }

  @Test
  def noFiltersForFieldTest(): Unit = {
    expectedException.expect(classOf[IllegalArgumentException])
    expectedException.expectMessage(s"No filers found for tsRangeFieldName $tsRangeFieldName")
    val df = sparkSession.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"user_id = 'user1'")
    val partitions = df.rdd.partitions
  }
}