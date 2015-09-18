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
package com.basho.riak.spark.examples.dataframes

import com.basho.riak.client.core.query.indexes.LongIntIndex
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.reflect.runtime.universe
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.Location
import com.basho.riak.spark.rdd.RiakFunctions

/**
 * Example shows how Spark DataFrames can be used with Riak
 * connector for spark
 */
object SimpleScalaRiakDataframesExample {
  private val namespace = new Namespace("users")

  case class UserData(user_id: String, name: String, age: Int, category: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Riak Spark Dataframes Example")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")

    val sc = new SparkContext(sparkConf)

    // Work with clear bucket
    clearBucket(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // To enable toDF()
    import sqlContext.implicits._

    // Save test data from json file to riak bucket
    val inputRDD = sqlContext.read.json("src/main/resources/test_data.json").toJSON.map {
      line =>
        val obj = RiakObjectConversionUtil.to(line)
        // RiakObjectConversionUtil.to() sets content type to text/plain if String is passed
        // Overwriting content type to application/json will allow automatic conversion to 
        // User defined type when reading from Riak
        obj.setContentType("application/json")
        obj
    }.saveToRiak(namespace)

    // Read from Riak with UDT to enable schema inference using reflection
    val df = sc.riakBucket[UserData](namespace).queryAll.toDF
    df.registerTempTable("users")

    println("count by category")
    df.groupBy("category").count.show

    println("sort by num of letters")
    // Register user defined function
    sqlContext.udf.register("stringLength", (s: String) => s.length)
    sqlContext.sql("select user_id, name, stringLength(name) nameLength from users order by nameLength").show

    println("filter age >= 21")
    sqlContext.sql("select * from users where age >= 21").show

  }

  private def clearBucket(sparkConf: SparkConf): Unit = {
    val rf = RiakFunctions(sparkConf)
    rf.withRiakDo(session => {
      rf.resetAndEmptyBucket(namespace)
    })
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }

}
