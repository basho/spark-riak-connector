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

import com.basho.riak.spark.rdd.RiakTSTests
import com.basho.riak.spark.toSparkContextFunctions
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.experimental.categories.Category

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
@Category(Array(classOf[RiakTSTests]))
class TimeSeriesReadTest extends AbstractTimeSeriesTest {
  import sparkSession.implicits._

  @Test
  def readDataAsSqlRow(): Unit = {
    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT user_id, temperature_k FROM $bucketName $sqlWhereClause")

    // -- verification
    val data = rdd.collect() map (r => r.toSeq)

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

  // TODO: Consider possibility of moving this case to the SparkDataframesTest
  @Test
  def riakTSRDDToDataFrame(): Unit = {
    val df = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName $sqlWhereClause")
      .map(r => TimeSeriesData(r.getTimestamp(0).getTime, r.getString(1), r.getDouble(2)))
      .toDF()

    df.createTempView("test")

    val data = sparkSession.sql("select * from test").toJSON.collect()

    // -- verification
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time:111111, user_id:'bryce', temperature_k:305.37},
        |   {time:111222, user_id:'bryce', temperature_k:300.12},
        |   {time:111333, user_id:'bryce', temperature_k:295.95},
        |   {time:111444, user_id:'ratman', temperature_k:362.121},
        |   {time:111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def riakTSRDDToDataFrameConvertTimestamp(): Unit = {
    val structType = StructType(List(
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val df = sc.riakTSTable[org.apache.spark.sql.Row](bucketName, schema = Some(structType))
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName $sqlWhereClause")
      .map(r => TimeSeriesData(r.getLong(0), r.getString(1), r.getDouble(2)))
      .toDF()

    df.createTempView("test")

    val data = sparkSession.sql("select * from test").toJSON.collect()

    // -- verification
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time:111111, user_id:'bryce', temperature_k:305.37},
        |   {time:111222, user_id:'bryce', temperature_k:300.12},
        |   {time:111333, user_id:'bryce', temperature_k:295.95},
        |   {time:111444, user_id:'ratman', temperature_k:362.121},
        |   {time:111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def sqlReadWithIncorrectSchemaShouldFail(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Provided schema contains fields that are not returned by query: unknown_field")

    val structType = StructType(List(
      StructField(name = "unknown_field", dataType = StringType))
    )

    sc.riakTSTable[org.apache.spark.sql.Row](bucketName, schema = Some(structType))
      .sql(s"SELECT * FROM $bucketName $sqlWhereClause")
      .map(r => TimeSeriesData(r.getLong(0), r.getString(1), r.getDouble(2)))
      .collect()
  }

  @Test
  def dataFrameGenericLoad(): Unit = {
    sparkSession.udf.register("getMillis", getMillis) // transforms timestamp to not deal with timezones

    val udfGetMillis = udf(getMillis)

    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      // For real usage no need to provide schema manually
      .schema(schema)
      .load(bucketName)
      .filter(s"time >= CAST('$fromStr' AS TIMESTAMP) AND time <= CAST('$toStr' AS TIMESTAMP) " +
        s"AND surrogate_key = 1 AND family = 'f'")
      // adding select statement to apply timestamp transformations to not deal with timezones
      .select(udfGetMillis($"time") as "time", $"family", $"surrogate_key", $"user_id", $"temperature_k")

    // -- verification
    val data = df.toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameReadShouldConvertTimestampToLong(): Unit = {
    val newSchema = StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val df = sparkSession.read
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(newSchema)
      .load(bucketName)
      .filter(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .select($"time", $"family", $"surrogate_key", $"user_id", $"temperature_k")

    // -- verification
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

    newSchema.foreach(sf => assertEquals(sf.dataType, df.schema(sf.name).dataType))
  }

  @Test
  def dataFrameReadWithIncorrectSchemaShouldFail(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Provided schema contains fields that are not returned by query: unknown_field")

    val structType = StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "family", dataType = StringType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType),
      StructField(name = "unknown_field", dataType = StringType))
    )

    sparkSession.read
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .schema(structType)
      .load(bucketName)
      .filter(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .select($"time", $"family", $"surrogate_key", $"user_id", $"temperature_k")
      .collect()
  }

  @Test
  def sqlReadSingleFieldShouldPass(): Unit = {
    sparkSession.read
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .format("org.apache.spark.sql.riak")
      .load(bucketName)
      .createTempView("test")

    val data = sparkSession
      .sql(
        s"""
           | SELECT user_id
           | FROM test
           | WHERE time >= CAST('$fromStr' AS TIMESTAMP)
           |   AND time <= CAST('$toStr' AS TIMESTAMP)
           |   AND surrogate_key = 1
           |   AND family = 'f'
           |""".stripMargin)
      .toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {user_id:'bryce'},
        |   {user_id:'bryce'},
        |   {user_id:'bryce'},
        |   {user_id:'ratman'},
        |   {user_id:'ratman'}
        |]""".stripMargin, stringify(data))
  }

  @Test
  def readColumnsWithoutSchema(): Unit = {
    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .select("time", "user_id", "temperature_k")
      .where(s"time >= $queryFromMillis AND time <= $queryToMillis AND surrogate_key = 1 AND family = 'f'")

    val data = rdd.map(r => TimeSeriesData(r.getTimestamp(0).getTime, r.getString(1), r.getDouble(2)))
      .toDF()
      .toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {time: 111555, user_id:'ratman', temperature_k:3502.212}
        ]""".stripMargin, stringify(data))
  }

  @Test
  def readColumnsWithSchema(): Unit = {
    val structType = StructType(List(
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName, schema = Some(structType))
      .select("time", "user_id", "temperature_k")
      .where(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")

    val data = rdd.map(r => TimeSeriesData(r.getLong(0), r.getString(1), r.getDouble(2)))
      .toDF()
      .toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {time: 111555, user_id:'ratman', temperature_k:3502.212}
        ]""".stripMargin, stringify(data))
  }

  @Test
  def readColumnsWithExtraFieldsInSchema(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Provided schema contains fields that are not returned by query: surrogate_key")

    val structType = StructType(List(
      StructField(name = "surrogate_key", dataType = LongType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName, schema = Some(structType))
      .select("time", "user_id", "temperature_k")
      .where(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .collect()
  }

  @Test
  def readColumnsWithUnknownFieldsInSchema(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Provided schema contains fields that are not returned by query: UNKNOWN_FIELD")

    val structType = StructType(List(
      StructField(name = "UNKNOWN_FIELD", dataType = LongType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName, schema = Some(structType))
      .select("time", "user_id", "temperature_k")
      .where(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .collect()
  }

  @Test
  def readColumnsWithMissedFieldsInSchema(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("Provided schema has nothing about the following fields returned by query: time")

    val structType = StructType(List(
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName, schema = Some(structType))
      .select("time", "user_id", "temperature_k")
      .where(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .collect()
  }

  @Test
  def readBySchemaWithoutDefinedColumns(): Unit = {
    val structType = StructType(List(
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .schema(structType)
      .where(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")

    val data = rdd.map(r => TimeSeriesData(r.getLong(0), r.getString(1), r.getDouble(2)))
      .toDF()
      .toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {time: 111555, user_id:'ratman', temperature_k:3502.212}
        ]""".stripMargin, stringify(data))
  }

  @Test
  def readBySchemaWithoutColumnsWithUnknownField(): Unit = {
    expectedException.expect(classOf[SparkException])
    expectedException.expectMessage("unexpected_select_field: unexpected field UNKNOWN_FIELD in select clause.")

    val structType = StructType(List(
      StructField(name = "UNKNOWN_FIELD", dataType = LongType),
      StructField(name = "time", dataType = LongType),
      StructField(name = "user_id", dataType = StringType),
      StructField(name = "temperature_k", dataType = DoubleType))
    )

    val rdd = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .schema(structType)
      .where(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .collect()
  }

  @Test
  def dataFrameReadShouldHandleTimestampAsLong(): Unit = {


    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useLong")
      .option("spark.riak.partitioning.ts-range-field-name", "time")
      .load(bucketName)

    assertEquals(LongType, df.schema("time").dataType)

    val data = df
      .filter(s"time > $queryFromMillis AND time < $queryToMillis AND surrogate_key = 1 AND family = 'f'")
      .select($"time", $"family", $"surrogate_key", $"user_id", $"temperature_k")
      .toJSON
      .collect()

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
}

@Category(Array(classOf[RiakTSTests]))
class TimeSeriesReadWithoutSchemaTest extends AbstractTimeSeriesTest {
  import sparkSession.implicits._

  @Test
  def riakTSRDDToDataFrame(): Unit = {
    val df = sc.riakTSTable[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName $sqlWhereClause")
      .map(r => TimeSeriesData(r.getTimestamp(0).getTime, r.getString(1), r.getDouble(2)))
      .toDF()

    df.createTempView("test")

    val data = sparkSession.sql("select * from test").toJSON.collect()

    // -- verification
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time:111111, user_id:'bryce', temperature_k:305.37},
        |   {time:111222, user_id:'bryce', temperature_k:300.12},
        |   {time:111333, user_id:'bryce', temperature_k:295.95},
        |   {time:111444, user_id:'ratman', temperature_k:362.121},
        |   {time:111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameReadShouldHandleTimestampAsTimestamp(): Unit = {

    sparkSession.udf.register("getMillis", getMillis) // transforms timestamp to not deal with timezones
    val udfGetMillis = udf(getMillis)

    val df = sparkSession.read
      .format("org.apache.spark.sql.riak")
      .option("spark.riakts.bindings.timestamp", "useTimestamp")
      .load(bucketName)

    assertEquals(TimestampType, df.schema("time").dataType)

    val data = df
      .filter(s"time > CAST('$fromStr' AS TIMESTAMP) AND time < CAST('$toStr' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'")
      .select(udfGetMillis($"time") as "time", $"family", $"surrogate_key", $"user_id", $"temperature_k")
      .toJSON
      .collect()

    assertEqualsUsingJSONIgnoreOrder(
      """[
        |   {surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }
}
