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

import com.basho.riak.spark.rdd.RiakTSTests
import org.junit.Test
import org.junit.experimental.categories.Category

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
@Category(Array(classOf[RiakTSTests]))
class SparkDataSetTest extends AbstractTimeSeriesTest {

  @Test
  def genericLoadAsDataSet(): Unit = {
    import sparkSession.implicits._

    val ds = sparkSession.read
      .format("org.apache.spark.sql.riak")
        .option("spark.riakts.bindings.timestamp", "useLong")
      .load(bucketName)
      .filter(filterExpression)
      .as[TimeSeriesData]

    val data: Array[TimeSeriesData] = ds.collect()

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
      """.stripMargin, data)
  }
}
