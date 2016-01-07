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
package com.basho.riak.spark.rdd.timeseries

import java.util.concurrent.ExecutionException
import java.util.{Calendar, GregorianCalendar, TimeZone}

import com.basho.riak.client.api.commands.timeseries.Delete
import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.operations.FetchBucketPropsOperation
import com.basho.riak.client.core.operations.ts.StoreOperation
import com.basho.riak.client.core.query.timeseries.{Cell, Row}
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.AbstractRDDTest
import org.junit.Assert._
import org.junit.Assume

import scala.collection.JavaConversions._

case class TimeSeriesData(time: Long, user_id: String, temperature_k: Double)

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
abstract class AbstractTimeSeriesTest(val createTestDate: Boolean = true) extends AbstractRDDTest {

  protected val DEFAULT_TS_NAMESPACE = new Namespace("time_series_test","time_series_test")
  protected val bucketName = DEFAULT_TS_NAMESPACE.getBucketTypeAsString

  val testData = List(
    TimeSeriesData(111111, "bryce", 305.37),
    TimeSeriesData(111222, "bryce", 300.12),
    TimeSeriesData(111333, "bryce", 295.95),

    TimeSeriesData(111444, "ratman", 362.121),
    TimeSeriesData(111555, "ratman", 3502.212)
  )

  val tsRangeStart: Calendar = mkTimestamp(testData.minBy(_.time).time)
  val tsRangeEnd: Calendar = mkTimestamp(testData.maxBy(_.time).time)

  val sqlWhereClause: String = s"WHERE time > ${tsRangeStart.getTimeInMillis - 5} AND " +
    s"time < ${tsRangeEnd.getTimeInMillis + 10} AND surrogate_key = 1 AND family = 'f'"

  val sqlQuery: String = s"SELECT surrogate_key, family, time, user_id, temperature_k " +
    s"FROM $bucketName $sqlWhereClause"

  final val msg = "Bucket type for Time Series test data is not created, Time series tests will be skipped"

  protected def mkTimestamp(timeInMillis: Long): Calendar = {
    val c = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    c.setTimeInMillis(timeInMillis)
    c
  }

  override def initialize(): Unit = {
    /* DO NOT CALL INHERITED initialize to avoid initialization of KV bucket */
    // super.initialize()

    sc = createSparkContext(initSparkConf())

    checkBucketExistence(DEFAULT_TS_NAMESPACE, msg + "\n\n" +
      "To create and activate TS test bucket, please use the following commands:\n" +
      "\t" + """./riak-admin bucket-type create time_series_test '{"props":{"n_val":3, "table_def": "create table time_series_test (surrogate_key sint64 not null, family varchar not null, time timestamp not null, user_id varchar not null, temperature_k double, primary key ((surrogate_key, family, quantum(time, 10, s)), surrogate_key, family, time))"}}' """  +"\n" +
      "\t./riak-admin bucket-type activate time_series_test")

    // ----------  Purging data: data might be not only created, but it may be also changed during the previous test case execution
    // Since there is no other options for truncation, data will be cleared for the time interval used for testing
    sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(sqlQuery)
      .collect()
      .map(row => (row.getLong(0), row.getString(1), row.getTimestamp(2).getTime))
      .map { case (surrogateKey, family, time) => List(new Cell(surrogateKey), new Cell(family), Cell.newTimestamp(time)) }
      .foreach { keys =>
        val builder = new Delete.Builder(DEFAULT_TS_NAMESPACE.getBucketNameAsString(), keys)
        withRiakDo(_.execute(builder.build()))
      }

    // Check that there is no date
    val collected = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(sqlQuery)
      .collect().length

    assertEquals(0, collected)

    // ----------  Storing test data into Riak TS
    if (createTestDate) {
      val tableName = DEFAULT_TS_NAMESPACE.getBucketType

      val rows = testData.map(f => new Row(
        new Cell(1) /*surrogate_key*/ ,
        new Cell("f") /* family */ ,
        Cell.newTimestamp(f.time),
        new Cell(f.user_id),
        new Cell(f.temperature_k)))

      val storeOp = new StoreOperation.Builder(tableName).withRows(rows).build()

      withRiakDo(session => {
        session.getRiakCluster.execute(storeOp).get()
      })
    }
  }

  protected def checkBucketExistence(ns: Namespace, warningMsg: String): Unit = {
    val fetchProps = new FetchBucketPropsOperation.Builder(ns).build()

    withRiakDo( session => {
      session.getRiakCluster.execute(fetchProps)
    })

    try {
      fetchProps.get().getBucketProperties
    } catch {
      case ex :ExecutionException if ex.getCause.isInstanceOf[RiakResponseException]
        && ex.getCause.getMessage.startsWith("No bucket-type named")  =>
        logWarning(warningMsg)
        Assume.assumeTrue(msg + " (See logs for the details)", false)
    }
  }

  protected def stringify = (s: Array[String]) => s.mkString("[", ",", "]")

}
