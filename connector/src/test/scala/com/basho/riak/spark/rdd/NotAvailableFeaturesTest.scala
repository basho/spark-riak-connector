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
package com.basho.riak.spark.rdd

import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.spark._
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.hamcrest.CustomTypeSafeMatcher
import org.junit.rules.ExpectedException
import org.junit.{ Rule, Test }
import org.junit.experimental.categories.Category

/**
  * Tests whether spark-riak-connector produces user friendly exception when TS or full bucket read queries are triggered
  * on Riak KV build which doesn't support them.
  * In order to run this test System property 'com.basho.riak-kv.pbchost' should be provided with address of started Riak KV.
  * If such system property is not provided this suite case will be skipped
  */
class NotAvailableFeaturesTest extends AbstractRiakSparkTest {
  val _expectedException: ExpectedException = ExpectedException.none()
  @Rule
  def expectedException: ExpectedException = _expectedException

  val coverageMatcher = new CustomTypeSafeMatcher[IllegalStateException]("match") {
    override def matchesSafely(t: IllegalStateException): Boolean = {
      t.getMessage.contains("Full bucket read is not supported on your version of Riak") &&
        t.getCause.isInstanceOf[RiakResponseException] &&
        t.getCause.getMessage.contains("Unknown message code: 70")
    }
  }

  val timeSeriesMatcher = new CustomTypeSafeMatcher[SparkException]("match") {
    override def matchesSafely(t: SparkException): Boolean = {
      t.getMessage.contains("Range queries are not supported in your version of Riak") &&
        t.getMessage.contains("Unknown message code: 90")
    }
  }

  @Category(Array(classOf[RiakKVTests],classOf[RiakKVNotAvailableFeaturesTest]))
  @Test
  def timeSeriesOnKV(): Unit = {
    expectedException.expect(timeSeriesMatcher)
    val rdd = sc.riakTSTable[Row]("bucket")
      .sql("select * from bucket")
      .collect()
  }

  @Category(Array(classOf[RiakKVTests],classOf[RiakKVNotAvailableFeaturesTest]))
  @Test
  def fullBucketReadOnKV(): Unit = {
    expectedException.expect(coverageMatcher)
    val rdd = sc.riakBucket[String](DEFAULT_NAMESPACE)
      .queryAll()
      .collect()
  }

  @Category(Array(classOf[RiakKVTests],classOf[RiakKVNotAvailableFeaturesTest]))
  @Test
  def queryRangeLocalOnKV(): Unit = {
    expectedException.expect(coverageMatcher)
    val rdd = sc.riakBucket[String](DEFAULT_NAMESPACE)
      .query2iRangeLocal("creationNo", 1, 1000)
      .collect()
  }
}
