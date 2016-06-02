package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.client.core.{RiakMessage, RiakNode}
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.failover.FailoverTest._
import com.basho.riak.spark.rdd.{AbstractRDDTest, RiakTSTests}
import com.basho.riak.stub.{ProxyMessageHandler, RiakNodeStub}
import com.basho.riak.test.cluster.DockerRiakCluster
import com.basho.riak.test.rule.DockerRiakClusterRule
import org.apache.spark.SparkConf
import org.junit.Assert._
import org.junit.experimental.categories.Category
import org.junit.{After, ClassRule, Test}
import shaded.com.basho.riak.protobuf.RiakMessageCodes._

import scala.collection.JavaConversions._

@Category(Array(classOf[RiakTSTests]))
class FailoverTest extends AbstractRDDTest {
  private val NUMBER_OF_TEST_VALUES = 1000

  private var stubNodes: Seq[RiakNodeStub] = Seq()

  protected override val jsonData = Some(asStrictJSON((1 to NUMBER_OF_TEST_VALUES)
    .map(i => Map("key" -> s"k$i", "value" -> s"v$i", "indexes" -> Map("creationNo" -> i))))) // scalastyle:ignore

  protected override def riakHosts: Set[HostAndPort] =
    HostAndPort.hostsFromString(cluster.getIps.mkString(","), RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet

  // Configure Spark using proxied hosts
  override protected def initSparkConf(): SparkConf = super.initSparkConf()
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

object FailoverTest {
  final val NODES_AMOUNT = 3
  final val STUBS_AMOUNT = 1

  @ClassRule
  def cluster: DockerRiakClusterRule = _cluster

  val _cluster: DockerRiakClusterRule = new DockerRiakClusterRule(DockerRiakCluster.builder()
      .withNodes(NODES_AMOUNT)
      .withTimeout(3))
}
