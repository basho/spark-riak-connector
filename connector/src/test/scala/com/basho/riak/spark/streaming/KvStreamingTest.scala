package com.basho.riak.spark.streaming

import java.nio.ByteBuffer
import java.util.concurrent._

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.{AbstractRiakSparkTest, RiakTSTests}
import org.junit.Assert.assertEquals
import org.junit.experimental.categories.Category
import org.junit.{After, Before, Test}

@Category(Array(classOf[RiakTSTests]))
class KvStreamingTest extends AbstractRiakSparkTest with SparkStreamingFixture {

  protected final val VALUES_NUMBER = 5

  protected final val executorService = Executors.newCachedThreadPool()
  private val dataSource = new SocketStreamingDataSource
  private var port = -1

  @Before
  def setUp(): Unit = {
    port = dataSource.start(client => {
      (0 until VALUES_NUMBER).foreach(i => client.write(ByteBuffer.wrap(s"value-$i\n".getBytes)))
      logInfo(s"$VALUES_NUMBER values were send to client")
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
          .saveToRiak(DEFAULT_NAMESPACE_4STORE.getBucketNameAsString)

        ssc.start()
        ssc.awaitTerminationOrTimeout(5 * 1000)
      }
    })

    val data: List[(String, Any)] = executorService.submit(new Callable[List[(String, Any)]] {
      override def call(): List[(String, Any)] = {
        var rdd = sc.riakBucket(DEFAULT_NAMESPACE_4STORE).queryAll()
        var count = rdd.count()
        while (count < VALUES_NUMBER) {
          TimeUnit.SECONDS.sleep(2)

          rdd = sc.riakBucket(DEFAULT_NAMESPACE_4STORE).queryAll()
          count = rdd.count()
        }
        rdd.collect().toList
      }
    }).get()

    assertEquals(VALUES_NUMBER, data.length)
    assertEqualsUsingJSONIgnoreOrder(
      """[
        | ['${json-unit.ignore}', 'value-0'],
        | ['${json-unit.ignore}', 'value-1'],
        | ['${json-unit.ignore}', 'value-2'],
        | ['${json-unit.ignore}', 'value-3'],
        | ['${json-unit.ignore}', 'value-4']
        | ]""".stripMargin, data)
  }
}