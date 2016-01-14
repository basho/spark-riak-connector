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

import com.basho.riak.spark.rdd.{AbstractRDDTest, RiakTSTests}
import com.basho.riak.spark.toSparkContextFunctions
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.riak.RiakSQLContext
import org.junit.Test
import org.junit.experimental.categories.Category

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
@Category(Array(classOf[RiakTSTests]))
class TimeSeriesReadTest extends AbstractTimeSeriesTest with AbstractRDDTest {

  @Test
  def readDataAsSqlRow(): Unit = {
    val rdd = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
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
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName $sqlWhereClause")
      .map(r=> TimeSeriesData(r.getTimestamp(0).getTime, r.getString(1), r.getDouble(2)))
      .toDF()

    df.registerTempTable("test")

    val data = sqlContext.sql("select * from test").toJSON.collect()

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
  def sqlRangeQuery(): Unit = {
    /*
     * This usage scenario requires to use RiakSQLContext, otherwise
     * RuntimeException('Table Not Found: time_series_test') will be thrown
     */
    val sqlContext = new RiakSQLContext(sc, DEFAULT_TS_NAMESPACE.getBucketTypeAsString)
    sqlContext.udf.register("getMillis", getMillis) // transforms timestamp to not deal with timezones
    val df: DataFrame = sqlContext.sql(
      s"SELECT getMillis(time) as time, user_id, temperature_k " +
        s" FROM $bucketName " +
        s" WHERE time > CAST($queryFrom AS TIMESTAMP) AND time < CAST($queryTo AS TIMESTAMP) " +
        s"        AND surrogate_key = 1 AND family = 'f'")

    // -- verification
    df.printSchema()
    val data = df.toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time: 111111, user_id:'bryce', temperature_k:305.37},
        |   {time: 111222, user_id:'bryce', temperature_k:300.12},
        |   {time: 111333, user_id:'bryce', temperature_k:295.95},
        |   {time: 111444, user_id:'ratman', temperature_k:362.121},
        |   {time: 111555, user_id:'ratman', temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }

  @Test
  def dataFrameGenericLoad(): Unit = {
    val sqlContext = new SQLContext(sc)
    sqlContext.udf.register("getMillis", getMillis) // transforms timestamp to not deal with timezones

    import org.apache.spark.sql.functions.udf
    import sqlContext.implicits._

    val udfGetMillis = udf(getMillis)

    val df = sqlContext.read
      .format("org.apache.spark.sql.riak")
      // For real usage no needs to provide schema manually
      .schema(schema)
      .load(bucketName)
      .filter(s"time > CAST($queryFrom AS TIMESTAMP) AND time < CAST($queryTo AS TIMESTAMP) " +
        s"AND surrogate_key = 1 AND family = 'f'")
      // adding select statement to apply timestamp transformations to not deal with timezones
      .select(udfGetMillis($"time") as "time", $"family", $"surrogate_key", $"user_id", $"temperature_k")

    // -- verification
    df.printSchema()
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
}
