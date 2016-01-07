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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.junit.Test
import org.junit.experimental.categories.Category

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

    sqlRowsRdd.saveToRiakTS(bucketName)

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

    val jsonRdd = sc.parallelize(Seq(
      """{"surrogate_key": 1, "family": "f", "time": 111111, "user_id": "bryce", "temperature_k": 305.37}""",
      """{"surrogate_key": 1, "family": "f", "time": 111222, "user_id": "bryce", "temperature_k": 300.12}""",
      """{"surrogate_key": 1, "family": "f", "time": 111333, "user_id": "bryce", "temperature_k": 295.95}""",
      """{"surrogate_key": 1, "family": "f", "time": 111444, "user_id": "ratman", "temperature_k": 362.121}""",
      """{"surrogate_key": 1, "family": "f", "time": 111555, "user_id": "ratman", "temperature_k": 3502.212}"""))

    val df = sqlContext.read.schema(
      StructType(List(
        StructField(name = "surrogate_key", dataType = IntegerType),
        StructField(name = "family", dataType = StringType),
        StructField(name = "time", dataType = LongType),
        StructField(name = "user_id", dataType = StringType),
        StructField(name = "temperature_k", dataType = DoubleType))))
      .json(jsonRdd)

    df.rdd.saveToRiakTS(DEFAULT_TS_NAMESPACE.getBucketTypeAsString)

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

}
