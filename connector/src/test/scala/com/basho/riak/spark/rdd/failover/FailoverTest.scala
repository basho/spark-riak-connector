package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.client.core.RiakMessage
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.{AbstractRiakSparkTest, RiakTSTests}
import com.basho.riak.stub.{ProxyMessageHandler, RiakNodeStub}
import com.basho.riak.test.rule.annotations.OverrideRiakClusterConfig
import org.apache.spark.SparkConf
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.{After, Test}
import shaded.com.basho.riak.protobuf.RiakMessageCodes._

@Category(Array(classOf[RiakTSTests]))
@OverrideRiakClusterConfig(nodes = 3, timeout = 5)
class FailoverTest extends AbstractRiakSparkTest {
  private val NUMBER_OF_TEST_VALUES = 1000
  private val STUBS_AMOUNT = 1

  private var stubNodes: Seq[RiakNodeStub] = _

  protected override val jsonData = Some(asStrictJSON((1 to NUMBER_OF_TEST_VALUES)
    .map(i => Map("key" -> s"k$i", "value" -> s"v$i", "indexes" -> Map("creationNo" -> i))))) // scalastyle:ignore

  // Configure Spark using proxied hosts
  override protected def initSparkConf(): SparkConf = {
    stubNodes = Seq()
    super.initSparkConf()
      .set("spark.riak.connection.host", riakHosts.map {
        case hp: HostAndPort if stubNodes.length < STUBS_AMOUNT =>
          val stub = RiakNodeStub(new ProxyMessageHandler(hp) {
            override def onRespond(input: RiakMessage, output: Iterable[RiakMessage]): Unit = input.getCode match {
              case MSG_CoverageReq => stubNodes.head.stop() // stop proxy node after coverage plan sent to client
              case _ => super.onRespond(input, output)
            }
          })
          stubNodes = stubNodes :+ stub
          stub.start()
        case hp: HostAndPort => hp
      }.map(hp => s"${hp.getHost}:${hp.getPort}").mkString(","))
  }

  @After
  override def destroySparkContext(): Unit = {
    Option(stubNodes).foreach(_.foreach(_.stop()))
    super.destroySparkContext()
  }

  @Test
  def fullBucketRead(): Unit ={
    val data = sc.riakBucket[String](DEFAULT_NAMESPACE).queryAll().collect()

    // verify total number of values
    val allValues = data.sortBy(x => x.substring(1).toLong)

    assertEquals(NUMBER_OF_TEST_VALUES, allValues.length)

    // verify returned values
    (1 to NUMBER_OF_TEST_VALUES).foreach(i => assertEquals( "v" + i, allValues(i - 1)))
  }
}