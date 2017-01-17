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
package com.basho.riak.spark.examples

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.rdd.RiakFunctions
import org.apache.spark.{SparkConf, SparkContext}
import com.basho.riak.spark._
import org.apache.spark.sql.SparkSession

/**
 * Really simple demo program which calculates the number of records loaded
 * from the Riak bucket
 */
object SimpleScalaRiakExample {
  private val SOURCE_DATA = "test-data"

  private val TEST_DATA: String =
    "[" +
        "  {key: 'key-1', indexes: {creationNo: 1}, value: 'value1'}" +
        ", {key: 'key-2', indexes: {creationNo: 2}, value: 'value2'}" +
        ", {key: 'key-3', indexes: {creationNo: 3}, value: 'value3'}" +
        ", {key: 'key-4', indexes: {creationNo: 4}, value: 'value4'}" +
        ", {key: 'key-5', indexes: {creationNo: 5}, value: 'value5'}" +
        ", {key: 'key-6', indexes: {creationNo: 6}, value: 'value6'}" +
    "]"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Simple Scala Riak Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")
    //setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:10017")


    println(s"Writing test data to Riak: \n $TEST_DATA")

    createTestData(sparkConf)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.riakBucket("test-data")
      .queryAll()

    println(s"Riak query result: \n ${rdd.foreach(println)}")
    println(s"Records in query result: ${rdd.count()}")
  }

  private def createTestData(sparkConf: SparkConf): Unit = {
    val rf = RiakFunctions(sparkConf)

    rf.withRiakDo(session => {
      rf.createValuesForBucket(session, SOURCE_DATA, TEST_DATA, true)
    })
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
}
