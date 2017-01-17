/**
  * Copyright (c) 2015-2016 Basho Technologies, Inc.
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

import java.io.{BufferedReader, InputStreamReader}

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.SparkJobCompletionTest._
import com.basho.riak.spark.rdd.connector.RiakConnectorConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.junit.Assert
import org.slf4j.LoggerFactory
import org.junit.experimental.categories.Category

import scala.util.matching.Regex

/**
  * The goal of this test is to prove that java process finishes right after Spark job completion.
  * This test was created in scope of https://bashoeng.atlassian.net/browse/SPARK-30
  * Test spawns separate JVM, it may introduce weird errors in case of running on CI
  */
@Category(Array(classOf[RiakTSTests]))
class SparkJobCompletionTest extends AbstractRiakSparkTest {

  protected override val jsonData = Some(
    """[
      |   {key: 'key-1', indexes: {creationNo: 1}, value: 'value1'},
      |   {key: 'key-2', indexes: {creationNo: 2}, value: 'value2'},
      |   {key: 'key-3', indexes: {creationNo: 3}, value: 'value3'},
      |   {key: 'key-4', indexes: {creationNo: 4}, value: 'value4'},
      |   {key: 'key-5', indexes: {creationNo: 5}, value: 'value5'},
      |   {key: 'key-6', indexes: {creationNo: 6}, value: 'value6'}
      |]""".stripMargin
  )

  @Test(timeout = 2 * 60 * 1000) // 2 minutes
  def testSparkJobCompletionTimeout(): Unit = {
    sc.stop()

    val classpath = System.getProperty("java.class.path")
    val riakHost = sc.getConf.get("spark.riak.connection.host")

    val jobClassName = SparkJobCompletionTest.getClass.getName.dropRight(1)

    val process = new ProcessBuilder("java", "-cp", classpath, jobClassName, riakHost, DEFAULT_NAMESPACE.getBucketTypeAsString, DEFAULT_NAMESPACE.getBucketNameAsString)
      .redirectErrorStream(true)
      .start

    val pls = new ProcessLoggingThread(process)
    pls.start()


    // force destroy job's process if current process compete
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = process.destroy()
    })

    process.waitFor() // wait for Spark job process complete

    pls.results match {
      case None => Assert.fail("No data was returned from the Spark Job, probably Spark Job failed")
      case Some(actualData: String) => assertEqualsUsingJSONIgnoreOrder(
        """[
          | ['key-1','value1'],
          | ['key-2','value2'],
          | ['key-3','value3'],
          | ['key-4','value4'],
          | ['key-5','value5'],
          | ['key-6','value6']
          | ]""".stripMargin, actualData)
    }
  }

  class ProcessLoggingThread(process: Process) extends Thread {
    setDaemon(true)

    val pattern = new Regex(s"${SparkJobCompletionTest.RESULTS_READ_FROM_SPARK}-(.*)")
    @volatile var results: Option[String] = None

    override def run(): Unit = {
      val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream))
      try {
        var line = Option(bufferedReader.readLine)
        while (line.isDefined) {
          pattern.findAllIn(line.get).matchData foreach {
            m => results = Some(m.group(1))
          }
          logger.debug("[Spark Job Process]: {}", line.get)

          line = Option(bufferedReader.readLine)
        }
      } finally {
        Option(bufferedReader).foreach(x => x.close())
      }
    }
  }
}

object SparkJobCompletionTest extends JsonFunctions {
  val RESULTS_READ_FROM_SPARK = "RESULTS_READ_FROM_SPARK"

  final val logger = LoggerFactory.getLogger(classOf[SparkJobCompletionTest])
  def main(args: Array[String]): Unit = {
    val ns = new Namespace(args(1), args(2))

    val sparkConf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set("spark.master", "local")
      .set("spark.riak.connection.host", args(0))
      .set("spark.riak.connections.inactivity.timeout",
        (RiakConnectorConf.defaultInactivityTimeout * 60 * 5).toString) // 5 minutes is enough time to complete Spark job

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val data = sparkSession.sparkContext.riakBucket(ns).queryAll().collect()

    // HACK: Results should be printed  for further analysis in the original JVM
    // to indicate that Spark job was completed successfully
    println(RESULTS_READ_FROM_SPARK + "-" + asStrictJSON(data) )
  }
}