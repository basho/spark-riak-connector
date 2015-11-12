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

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.After

abstract class AbstractRiakSparkTest extends AbstractRiakTest {
  // SparkContext, created per test case
  protected var sc:SparkContext = null

  protected def initSparkConf():SparkConf = new SparkConf(false)
    .setMaster("local")
    .setAppName(getClass.getSimpleName)
    .set("spark.riak.connection.host", DEFAULT_RIAK_HOST)
    .set("spark.riak.output.wquorum", "1")
    .set("spark.riak.input.fetch-size", "2")

  override def initialize(): Unit = {
    super.initialize()

    sc = createSparkContext(initSparkConf())
  }

  protected def createSparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }

  @After
  def destroySparkContext(): Unit = {
    if(sc != null){
      sc.stop()
    }
  }
}
