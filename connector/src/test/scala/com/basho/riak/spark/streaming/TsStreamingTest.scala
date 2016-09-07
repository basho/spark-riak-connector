package com.basho.riak.spark.streaming

import java.nio.ByteBuffer
import java.util.concurrent.{Callable, Executors, TimeUnit}

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.RiakTSTests
import com.basho.riak.spark.rdd.timeseries.{AbstractTimeSeriesTest, TimeSeriesData}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.Row
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.{After, Before, Test}

@Category(Array(classOf[RiakTSTests]))
class TsStreamingTest extends AbstractTimeSeriesTest(false) with SparkStreamingFixture {

  protected final val executorService = Executors.newCachedThreadPool()
  private val dataSource = new SocketStreamingDataSource
  private var port = -1

  @Before
  def setUp(): Unit = {
    port = dataSource.start(client => {
      testData
        .map(tolerantMapper.writeValueAsString)
        .foreach(x => client.write(ByteBuffer.wrap(s"$x\n".getBytes)))
      logInfo(s"${testData.length} values were send to client")
    })
  }

  @After
  def tearDown(): Unit = {
    dataSource.stop()
  }

  @Test(timeout = 10 * 1000) // 10 seconds timeout
  def saveToRiak(): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = {
        ssc.socketTextStream("localhost", port)
          .map(string => {
            val tsdata = new ObjectMapper()
              .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
              .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
              .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
              .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
              .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
              .registerModule(DefaultScalaModule)
              .readValue(string, classOf[TimeSeriesData])
            Row(1, "f", tsdata.time, tsdata.user_id, tsdata.temperature_k)
          })
          .saveToRiakTS(bucketName)

        ssc.start()
        ssc.awaitTerminationOrTimeout(5 * 1000)
      }
    })

    val result = executorService.submit(new Callable[Array[Seq[Any]]] {
      override def call(): Array[Seq[Any]] = {
        var rdd = sc.riakTSTable[Row](bucketName)
          .sql(s"SELECT user_id, temperature_k FROM $bucketName $sqlWhereClause")
        var count = rdd.count()
        while (count < testData.length) {
          TimeUnit.SECONDS.sleep(2)

          rdd = sc.riakTSTable[Row](bucketName)
            .sql(s"SELECT user_id, temperature_k FROM $bucketName $sqlWhereClause")
          count = rdd.count()
        }
        rdd.collect().map(_.toSeq)
      }
    }).get()

    assertEquals(testData.length, result.length)
    assertEqualsUsingJSONIgnoreOrder(
      """
        |[
        |   ['bryce',305.37],
        |   ['bryce',300.12],
        |   ['bryce',295.95],
        |   ['ratman',362.121],
        |   ['ratman',3502.212]
        |]
      """.stripMargin, result)
  }
}
