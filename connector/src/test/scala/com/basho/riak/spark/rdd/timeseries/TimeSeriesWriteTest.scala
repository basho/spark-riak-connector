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

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.RiakTSTests
import com.basho.riak.spark.writer.WriteDataMapperFactory._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.Assert._

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
@Category(Array(classOf[RiakTSTests]))
class TimeSeriesWriteTest extends AbstractTimeSeriesTest(false) {

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
    val newRdd = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
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
  def saveDataFrameWithSchemaToRiak(): Unit = {
    val sqlContext = new SQLContext(sc)

    val sourceDF = getSourceDF(sqlContext)
    sourceDF.rdd.saveToRiakTS(DEFAULT_TS_NAMESPACE.getBucketTypeAsString)

    // -- verification
    val newRdd = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
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
    val sqlContext = new SQLContext(sc)
    val sourceDF =  getSourceDF(sqlContext)

    sourceDF.write
      .format("org.apache.spark.sql.riak")
      .mode(SaveMode.Append)
      .save(bucketName)

    // -- verification
    val df = sqlContext.read
      .format("org.apache.spark.sql.riak")
      .schema(schema)
      .load(bucketName)
      .filter(s"time > CAST($queryFrom AS TIMESTAMP) AND time < CAST($queryTo AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")

    val data = df.toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {surrogate_key:1, family: 'f', time: '1970-01-01 03:01:51.111', user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: '1970-01-01 03:01:51.222', user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: '1970-01-01 03:01:51.333', user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: '1970-01-01 03:01:51.444', user_id:'ratman',temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: '1970-01-01 03:01:51.555', user_id:'ratman',temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  private def getSourceDF(sqlContext: SQLContext): DataFrame = {
    val rdd: RDD[Row] = sqlContext.sparkContext.parallelize(sparkRowsWithSchema)
    sqlContext.createDataFrame(rdd, schema)
  }
}
