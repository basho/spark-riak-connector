/*******************************************************************************
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
 *******************************************************************************/
package com.basho.riak.spark.rdd.timeseries

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.RiakTSTests
import com.basho.riak.spark.writer.WriteDataMapperFactory._
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.junit.Assert._
import org.junit.Test
import org.junit.experimental.categories.Category
import com.basho.riak.spark.util.TSConversionUtil

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
@Category(Array(classOf[RiakTSTests]))
class TimeSeriesWriteTest extends AbstractTimeSeriesTest(false) {
  import sparkSession.implicits._

  @Test
  def saveSqlRowsToRiak(): Unit = {
    val sqlRowsRdd = sc.parallelize(
      Seq(
        Row(1, "f", 111111L, "bryce", 305.37),
        Row(1, "f", 111222L, "bryce", 300.12),
        Row(1, "f", 111333L, "bryce", 295.95),
        Row(1, "f", 111444L, "ratman", 362.121),
        Row(1, "f", 111555L, "ratman", 3502.212)))


    // check to be 100% sure that schema is not provided
    sqlRowsRdd.collect().foreach( r => assertNull(r.schema) )

    sqlRowsRdd.saveToRiakTS(bucketName)

    // -- verification
    val newRdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT user_id, temperature_k FROM $bucketName $sqlWhereClause")

    val data = newRdd.collect().map(_.toSeq)

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   ['bryce',305.37],
        |   ['bryce',300.12],
        |   ['bryce',295.95],
        |   ['ratman',362.121],
        |   ['ratman',3502.212]
        |]
      """.stripMargin, data)
  }
  
  @Test
  def saveInvalidDataToRiak(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Invalid data found at row index(es) 1")

    sc.parallelize(Seq(Row("invalid", "data", "types")))
      .saveToRiakTS(bucketName)
  }

  @Test
  def saveDataFrameWithSchemaToRiak(): Unit = {
    val sourceDF = getSourceDF(sparkSession)
    sourceDF.rdd.saveToRiakTS(DEFAULT_TS_NAMESPACE.getBucketTypeAsString)

    // -- verification
    val newRdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT user_id, temperature_k FROM $bucketName $sqlWhereClause")

    val data = newRdd.collect().map(_.toSeq)

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   ['bryce',305.37],
        |   ['bryce',300.12],
        |   ['bryce',295.95],
        |   ['ratman',362.121],
        |   ['ratman',3502.212]
        |]
      """.stripMargin, data)
  }

  @Test
  def dataFrameGenericSave(): Unit = {
    val udfGetMillis = udf(getMillis)

    val sourceDF =  getSourceDF(sparkSession)

    sourceDF.write
      .format("org.apache.spark.sql.riak")
      .mode(SaveMode.Append)
      .save(bucketName)

    // -- verification
    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >= CAST('$fromStr' AS TIMESTAMP) AND time <= CAST('$toStr' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")
      // adding select statement to apply timestamp transformations
      .select(udfGetMillis($"time") as "time", $"family", $"surrogate_key", $"user_id", $"temperature_k")

    val data = df.toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameWriteWithTimeFieldAsLongShouldPass(): Unit = {
    import org.apache.spark.sql.functions.udf
    val udfGetMillis = udf(getMillis)

    val sourceDF = getSourceDF(sparkSession, StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    ))

    sourceDF.write
      .format("org.apache.spark.sql.riak")
      .mode(SaveMode.Append)
      .save(bucketName)

    // -- verification
    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >= CAST('$fromStr' AS TIMESTAMP) AND time <= CAST('$toStr' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")
      // adding select statement to apply timestamp transformations
      .select(udfGetMillis($"time") as "time", $"family", $"surrogate_key", $"user_id", $"temperature_k")

    val data = df.toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameWriteWithEmptyCells(): Unit = {
    val udfGetMillis = udf(getMillis)

    val tsRows = Seq[org.apache.spark.sql.Row] (
      org.apache.spark.sql.Row(2L, "f", 111111L, "test", null),
      org.apache.spark.sql.Row(2L, "f", 111222L, "test", 123.123),
      org.apache.spark.sql.Row(2L, "f", 111333L, "test", 345.34)
    )
    val schema = StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType)))

    val initialDF = getInitialDF(sparkSession, schema, tsRows)

    initialDF.write
      .format("org.apache.spark.sql.riak")
      .mode(SaveMode.Append)
      .save(bucketName)

    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 2 AND family = 'f'")

    val data = df.toJSON.collect()
    
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {surrogate_key:2, family: 'f', time: 111111, user_id:'test'},
        |   {surrogate_key:2, family: 'f', time: 111222, user_id:'test', temperature_k:123.123},
        |   {surrogate_key:2, family: 'f', time: 111333, user_id:'test', temperature_k:345.34}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameFailOnWritingEmptyTime(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Invalid data found at row index(es)")

    val tsRows = Seq[org.apache.spark.sql.Row] (
      org.apache.spark.sql.Row(2L, "f", null, "test", 123.123)
    )

    val initialDF = getInitialDF(sparkSession, StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    ), tsRows)
      initialDF.write
        .format("org.apache.spark.sql.riak")
        .mode(SaveMode.Append)
        .save(bucketName)
  }

  @Test
  def dataFrameFailOnWritingEmptySeries(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Invalid data found at row index(es)")

    val tsRows = Seq[org.apache.spark.sql.Row] (
      org.apache.spark.sql.Row(null, "f", 111222L, "test", 123.123)
    )

    val initialDF = getInitialDF(sparkSession, StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    ), tsRows)
      initialDF.write
        .format("org.apache.spark.sql.riak")
        .mode(SaveMode.Append)
        .save(bucketName)
  }

  @Test
  def dataFrameFailOnWritingEmptyFamily(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Invalid data found at row index(es)")

    val tsRows = Seq[org.apache.spark.sql.Row] (
      org.apache.spark.sql.Row(2L, null, 111222L, "test", 123.123)
    )

    val initialDF = getInitialDF(sparkSession, StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    ), tsRows)
      initialDF.write
        .format("org.apache.spark.sql.riak")
        .mode(SaveMode.Append)
        .save(bucketName)
  }

  private def getSourceDF(sparkSession: SparkSession, structType:StructType = schema): DataFrame = {
    val sparkRowsWithSchema = riakTSRows.map( r => TSConversionUtil.asSparkRow(structType, r))
    val rdd: RDD[Row] = sparkSession.sparkContext.parallelize(sparkRowsWithSchema)
    sparkSession.createDataFrame(rdd, structType)
  }

  private def getInitialDF(sparkSession: SparkSession, structType:StructType = schema, rows: Seq[Row]): DataFrame = {
    val rdd: RDD[Row] = sparkSession.sparkContext.parallelize(rows)
    sparkSession.createDataFrame(rdd, structType)
  }
}
