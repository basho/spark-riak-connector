/**
  * Copyright (c) 2015-2017 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.basho.riak.spark.rdd.timeseries

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util._
import java.util.concurrent.TimeUnit

import com.basho.riak.client.api.commands.timeseries.{CreateTable, Delete}
import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.operations.FetchBucketPropsOperation
import com.basho.riak.client.core.operations.ts.{QueryOperation, StoreOperation}
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.timeseries.Cell
import com.basho.riak.client.core.query.timeseries.ColumnDescription
import com.basho.riak.client.core.query.timeseries.FullColumnDescription
import com.basho.riak.client.core.query.timeseries.Row
import com.basho.riak.client.core.query.timeseries.TableDefinition
import com.basho.riak.spark.rdd.AbstractRiakSparkTest
import org.apache.spark.riak.Logging
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Rule
import org.junit.rules.ExpectedException

import scala.collection.JavaConversions._
import scala.util.control.Exception._

case class TimeSeriesData(time: Long, user_id: String, temperature_k: Double)

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
abstract class AbstractTimeSeriesTest(val createTestData: Boolean = true) extends AbstractRiakSparkTest with Logging {

  val _expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def expectedException: ExpectedException = _expectedException

  protected def getMillis = (t: Timestamp) => t.getTime

  protected val DEFAULT_TS_NAMESPACE = new Namespace("time_series_test", "time_series_test")
  protected val bucketName = DEFAULT_TS_NAMESPACE.getBucketTypeAsString

  val schema = StructType(Seq(
    StructField(name = "surrogate_key", dataType = LongType, nullable = false),
    StructField(name = "family", dataType = StringType, nullable = false),
    StructField(name = "time", dataType = TimestampType, nullable = false),
    StructField(name = "user_id", dataType = StringType, nullable = false),
    StructField(name = "temperature_k", dataType = DoubleType))
  )

  val tableDefinition: TableDefinition = new TableDefinition(DEFAULT_TS_NAMESPACE.getBucketNameAsString, Seq(
    new FullColumnDescription("surrogate_key", ColumnDescription.ColumnType.SINT64, false, 1),
    new FullColumnDescription("family", ColumnDescription.ColumnType.VARCHAR, false, 2),
    new FullColumnDescription("time", ColumnDescription.ColumnType.TIMESTAMP, false, 3),
    new FullColumnDescription("user_id", ColumnDescription.ColumnType.VARCHAR, false),
    new FullColumnDescription("temperature_k", ColumnDescription.ColumnType.DOUBLE, true)
  ))

  val testData = Seq(
    TimeSeriesData(111111, "bryce", 305.37),
    TimeSeriesData(111222, "bryce", 300.12),
    TimeSeriesData(111333, "bryce", 295.95),
    TimeSeriesData(111444, "ratman", 362.121),
    TimeSeriesData(111555, "ratman", 3502.212)
  )

  val tsRangeStart: Calendar = mkTimestamp(testData.minBy(_.time).time)
  val tsRangeEnd: Calendar = mkTimestamp(testData.maxBy(_.time).time)

  val queryFromMillis = tsRangeStart.getTimeInMillis - 5
  val queryToMillis = tsRangeEnd.getTimeInMillis + 10

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
  val fromStr = dateFormat.format(new Date(queryFromMillis)) // for spark 1.6
  val toStr = dateFormat.format(new Date(queryToMillis)) // for spark 1.6

  val riakTSRows = testData.map(f => new Row(
    new Cell(1) /* surrogate_key */ ,
    new Cell("f") /* family */ ,
    Cell.newTimestamp(f.time),
    new Cell(f.user_id),
    new Cell(f.temperature_k))
  )

  final val filterExpression = s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'"
  final val sqlWhereClause = s"WHERE $filterExpression"

  final val sqlQuery = s"SELECT surrogate_key, family, time, user_id, temperature_k FROM $bucketName $sqlWhereClause"

  final val msg = "Bucket type for Time Series test data is not created, Time series tests will be skipped"

  protected def mkTimestamp(timeInMillis: Long): Calendar = {
    val c = new GregorianCalendar(TimeZone.getTimeZone("UTC"))
    c.setTimeInMillis(timeInMillis)
    c
  }

  override def setupData(): Unit = {
    /* DO NOT CALL INHERITED initialize to avoid initialization of KV bucket */
    // super.setupData()

    createTableIfNotExists(DEFAULT_TS_NAMESPACE)

    // ----------  Purging data: data might be not only created, but it may be also changed during the previous test case execution
    // Since there is no other options for truncation, data will be cleared for the time interval used for testing
    val operationBuilder = new QueryOperation.Builder(sqlQuery)
    withRiakDo(session => {
      session.getRiakCluster.execute(operationBuilder.build()).get().getRowsCopy
        .map(row => Seq(row.getCellsCopy.get(0), row.getCellsCopy.get(1), row.getCellsCopy.get(2)))
        .foreach { keys =>
          val builder = new Delete.Builder(DEFAULT_TS_NAMESPACE.getBucketNameAsString(), keys)
          withRiakDo(_.execute(builder.build()))
        }

      val collected = session.getRiakCluster.execute(operationBuilder.build()).get().getRowsCopy
      assertTrue(collected.isEmpty)
    })

    // ----------  Storing test data into Riak TS
    if (createTestData) {
      val tableName = DEFAULT_TS_NAMESPACE.getBucketTypeAsString

      val storeOp = new StoreOperation.Builder(tableName)
        .withRows(riakTSRows)
        .build()

      withRiakDo(session => {
        session.getRiakCluster.execute(storeOp).get()
      })
    }
  }

  protected def createTableIfNotExists(ns: Namespace): Unit = {
    val fetchProps = new FetchBucketPropsOperation.Builder(ns).build()
    withRiakDo(session => {
      session.getRiakCluster.execute(fetchProps)
      allCatch either fetchProps.get.getBucketProperties match {
        case Right(_) =>
          logDebug(s"Table with name '${ns.getBucketTypeAsString}' already exists. Creation is not required.")
        case Left(ex) if ex.getCause.isInstanceOf[RiakResponseException]
          && ex.getCause.getMessage.startsWith("No bucket-type named") =>
          logInfo(s"Table '${ns.getBucketTypeAsString}' is not found. New one will be created.")
          session.execute(new CreateTable.Builder(tableDefinition)
            .withQuantum(10, TimeUnit.SECONDS) // scalastyle:ignore
            .build())
        case Left(ex) => throw ex
      }
    })
  }

}
