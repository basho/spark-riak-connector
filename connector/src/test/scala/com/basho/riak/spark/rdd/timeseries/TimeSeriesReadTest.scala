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

import com.basho.riak.spark.rdd.RiakTSTests
import com.basho.riak.spark.toSparkContextFunctions
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[RiakTSTests]))
class TimeSeriesReadTest extends AbstractTimeSeriesTest {

  @Test
  def readDataAsSqlRow(): Unit ={
    val from = tsRangeStart.getTimeInMillis - 5
    val to = tsRangeEnd.getTimeInMillis + 10

    val bucketName =  DEFAULT_TS_NAMESPACE.getBucketTypeAsString
    val rdd = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT user_id, temperature_k FROM $bucketName WHERE time > $from AND time < $to AND surrogate_key = 1 AND family = 'f'")

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

  private def stringify = (s: Array[String]) => s.mkString("[", ",", "]")

  // TODO: Consider possibility of moving this case to the SparkDataframesTest
  @Test
  def riakTSRDDToDataFrame(): Unit ={
    val from = tsRangeStart.getTimeInMillis - 5
    val to = tsRangeEnd.getTimeInMillis + 10

    val bucketName =  DEFAULT_TS_NAMESPACE.getBucketTypeAsString


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.riakTSBucket[org.apache.spark.sql.Row](bucketName)
      .sql(s"SELECT time, user_id, temperature_k FROM $bucketName WHERE time > $from AND time < $to AND surrogate_key = 1 AND family = 'f'")
      .map(r=> TimeSeriesData(r.getLong(0), r.getString(1), r.getDouble(2)))
      .toDF()

    df.registerTempTable("test")

    val data = sqlContext.sql("select * from test").toJSON.collect()

    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   {time:111111, user_id:'bryce', temperature_k:305.37},
        |   {time:111222, user_id:'bryce', temperature_k:300.12},
        |   {time:111333, user_id:'bryce', temperature_k:295.95},
        |   {time:111444, user_id:'ratman',temperature_k:362.121},
        |   {time:111555, user_id:'ratman',temperature_k:3502.212}
        |]
      """.stripMargin, stringify(data))
  }
}
