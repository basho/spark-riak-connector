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
import java.util

import scala.annotation.migration
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.GreaterThanOrEqual
import org.apache.spark.sql.sources.LessThan
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType
import org.junit.{After, Rule, Test}
import org.junit.rules.ExpectedException
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import com.basho.riak.client.api.commands.timeseries.CoveragePlan
import com.basho.riak.client.core.query.timeseries.CoverageEntry
import com.basho.riak.client.core.query.timeseries.CoveragePlanResult
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.rdd.RiakTSRDD
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.connector.RiakSession
import com.basho.riak.spark.rdd.partitioner.RiakTSPartition
import com.basho.riak.spark.toSparkContextFunctions

import junit.framework.Assert.assertEquals

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

  val filters: Array[Filter] = Array(
    GreaterThanOrEqual("time", from),
    LessThan("time", to),
    EqualTo("user_id", "user1"))

  val sparkConf = new SparkConf().setAppName("Riak TS Spark Dataframes Example").setMaster("local")

  val sc = new SparkContext(sparkConf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

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
    assertEquals(partitionsCount, partitions.size)
  }
  

  @Test
  def smallRangeShouldBeSinglePartitionTest(): Unit = {
    val (localFrom, localTo) = (new Timestamp(500L), new Timestamp(504L))
    val df = sqlContext.read
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
    val df = sqlContext.read
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
    val df = sqlContext.read
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
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >  CAST('$from' AS TIMESTAMP) AND time <= CAST('$to' AS TIMESTAMP)")

    val partitions = df.rdd.partitions
    assertEquals(partitionsCount, partitions.size)
    assertEquals("SELECT time, user_id, temperature_k FROM bucket  WHERE time >= 10001 AND time < 10002",
      partitions.head.asInstanceOf[RiakTSPartition].queryData.head.sql)
    assertEquals("SELECT time, user_id, temperature_k FROM bucket  WHERE time >= 10015 AND time < 10016",
      partitions.last.asInstanceOf[RiakTSPartition].queryData.head.sql)
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
    assertEquals(partitionsCount, partitions.size)
    assertEquals("SELECT time, user_id, temperature_k FROM bucket  WHERE time >= 10000 AND time < 10001",
      partitions.head.asInstanceOf[RiakTSPartition].queryData.head.sql)
    assertEquals("SELECT time, user_id, temperature_k FROM bucket  WHERE time >= 10014 AND time < 10015",
      partitions.last.asInstanceOf[RiakTSPartition].queryData.head.sql)
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
  def withLessThanQuantaLimitTest(): Unit = {
    val df = sqlContext.read
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
    val df = sqlContext.read
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
    val df = sqlContext.read
      .option("spark.riak.input.split.count", partitionsCount.toString)
      .option("spark.riak.partitioning.ts-range-field-name", tsRangeFieldName)
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"user_id = 'user1'")
    val partitions = df.rdd.partitions
  }

  @Test
  def coveragePlanBasedPartitioningLessThanSplitCount(): Unit = {

    val connector = spy(RiakConnector(new SparkConf()))
    val session = mock(classOf[RiakSession])
    val coveragePlan = mock(classOf[CoveragePlanResult])
    doReturn(session).when(connector).openSession(Some(Seq(any(classOf[HostAndPort]))))
    when(session.execute(any(classOf[CoveragePlan]))).thenReturn(coveragePlan)

    val mapCE = createCoverageEntry(3, 3)
    mapCE.keys.foreach(h => when(coveragePlan.hostEntries(h)).thenAnswer(new Answer[util.List[CoverageEntry]] {
      override def answer(invocation: InvocationOnMock): util.List[CoverageEntry] =
        mapCE(h).asJava
    }))
    when(coveragePlan.iterator()).thenAnswer(new Answer[util.Iterator[_]] {
      override def answer(invocation: InvocationOnMock): util.Iterator[_] = mapCE.values.flatten.iterator.asJava
    })
    when(coveragePlan.hosts()).thenReturn(setAsJavaSet(mapCE.keySet))
    val rdd = new RiakTSRDD[Row](sc, connector, bucketName, Some(schema), None, None, filters)
    val partitions = rdd.partitions

    assertEquals(3, partitions.size)
  }
  
  @Test
  def coveragePlanBasedPartitioningGreaterThanSplitCount(): Unit = {

    val connector = spy(RiakConnector(new SparkConf()))
    val session = mock(classOf[RiakSession])
    val coveragePlan = mock(classOf[CoveragePlanResult])
    doReturn(session).when(connector).openSession(Some(Seq(any(classOf[HostAndPort]))))
    when(session.execute(any(classOf[CoveragePlan]))).thenReturn(coveragePlan)

    val mapCE = createCoverageEntry(100, 3)
    mapCE.keys.foreach(h => when(coveragePlan.hostEntries(h)).thenAnswer(new Answer[util.List[CoverageEntry]] {
      override def answer(invocation: InvocationOnMock): util.List[CoverageEntry] =
        mapCE(h).asJava
    }))
    when(coveragePlan.iterator()).thenAnswer(new Answer[util.Iterator[_]] {
      override def answer(invocation: InvocationOnMock): util.Iterator[_] = mapCE.values.flatten.iterator.asJava
    })
    when(coveragePlan.hosts()).thenReturn(setAsJavaSet(mapCE.keySet))
    val rdd = new RiakTSRDD[Row](sc, connector, bucketName, Some(schema), None, None, filters)
    val partitions = rdd.partitions

    assertEquals(10, partitions.size)
  }

  private def createCoverageEntry(numOfEntries: Int, numOfHosts: Int): Map[HostAndPort, IndexedSeq[CoverageEntry]] = {
    val ces = (1 to numOfEntries).map { i =>
      val ce = new CoverageEntry();
      ce.setCoverageContext(Array.emptyByteArray);
      ce.setFieldName("time");
      ce.setLowerBound(i * 1000);
      ce.setLowerBoundInclusive(true);
      ce.setUpperBound(i * 2000);
      ce.setUpperBoundInclusive(false);
      ce.setDescription(s"${bucketName} / time >= ${i * 1000} time < ${i * 200}");
      ce.setHost("localhost");
      ce.setPort((8080 + i % numOfHosts).toInt);
      ce
    }
    ces.groupBy(ce => HostAndPort.fromParts(ce.getHost, ce.getPort))
  }
}