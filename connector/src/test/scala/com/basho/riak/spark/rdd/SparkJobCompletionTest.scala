package com.basho.riak.spark.rdd

import java.io.{BufferedReader, InputStreamReader}

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.SparkJobCompletionTest._
import com.basho.riak.spark.rdd.connector.RiakConnectorConf
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test
import org.slf4j.LoggerFactory
import com.basho.riak.cluster._
import org.junit.experimental.categories.Category

/**
  * The aim of this test is verify that java process finishes right after Spark job completion.
  * This test was created in scope of https://bashoeng.atlassian.net/browse/SPARK-30
  */
@Category(Array(classOf[RiakTSTests]))
class SparkJobCompletionTest {

  @Test(timeout = 2 * 60 * 1000) // 2 minutes
  def testSparkJobCompletionTimeout(): Unit = withDockerizedRiak { dockerCluster =>
    val classpath = System.getProperty("java.class.path")
    val riakHost = dockerCluster.getIps.iterator().next()
    val jobClassName = SparkJobCompletionTest.getClass.getName.dropRight(1)

    val process = new ProcessBuilder("java", "-cp", classpath, jobClassName, riakHost)
      .redirectErrorStream(true)
      .start
    new ProcessLoggingThread(process).start()

    // force destroy job's process if current process compete
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = process.destroy()
    })

    process.waitFor() // wait for Spark job process complete
  }

  class ProcessLoggingThread(process: Process) extends Thread {
    setDaemon(true)

    override def run(): Unit = {
      val bufferedReader: BufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream))
      try {
        var line = Option(bufferedReader.readLine)
        while (line.isDefined) {
          logger.debug("[Spark Job Process]: {}", line.get)
          line = Option(bufferedReader.readLine)
        }
      } finally {
        Option(bufferedReader).foreach(x => x.close())
      }
    }
  }
}

object SparkJobCompletionTest {

  final val logger = LoggerFactory.getLogger(classOf[SparkJobCompletionTest])

  private final val NAMESPACE = new Namespace("test-data")
  private final val TEST_DATA: String =
    "[" +
      "  {key: 'key-1', indexes: {creationNo: 1}, value: 'value1'}" +
      ", {key: 'key-2', indexes: {creationNo: 2}, value: 'value2'}" +
      ", {key: 'key-3', indexes: {creationNo: 3}, value: 'value3'}" +
      ", {key: 'key-4', indexes: {creationNo: 4}, value: 'value4'}" +
      ", {key: 'key-5', indexes: {creationNo: 5}, value: 'value5'}" +
      ", {key: 'key-6', indexes: {creationNo: 6}, value: 'value6'}" +
      "]"

  def main(args: Array[String]): Unit = {
    def createTestData(sparkConf: SparkConf): Unit = {
      val rf = RiakFunctions(sparkConf)
      rf.withRiakDo(session => rf.createValues(session, NAMESPACE, TEST_DATA, purgeBucketBefore = true))
    }

    val sparkConf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .set("spark.master", "local")
      .set("spark.riak.connection.host", args.mkString(","))
      .set("spark.riak.connections.inactivity.timeout",
        (RiakConnectorConf.defaultInactivityTimeout * 60 * 5).toString) // 5 minutes

    createTestData(sparkConf)

    new SparkContext(sparkConf).riakBucket(NAMESPACE).queryAll().collect().length
  }
}
