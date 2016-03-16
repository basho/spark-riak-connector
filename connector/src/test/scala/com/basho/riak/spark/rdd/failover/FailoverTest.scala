package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.client.core.{RiakMessage, RiakNode}
import com.basho.riak.spark.rdd.failover.FailoverTest._
import com.basho.riak.spark.rdd.{FullBucketReadTest, RiakTSTests}
import com.basho.riak.stub.{ProxyMessageHandler, RiakNodeStub}
import com.basho.riak.test.cluster.DockerRiakCluster
import com.basho.riak.test.rule.DockerRiakClusterRule
import org.apache.spark.SparkConf
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, ClassRule}
import shaded.com.basho.riak.protobuf.RiakMessageCodes._

import scala.collection.JavaConversions._

@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakTSTests]))
class FailoverTest(splitSize: Int) extends FullBucketReadTest(splitSize) {

  private var stubNodes: Seq[RiakNodeStub] = Seq()

  // Configure riakHosts, using docker cluster hosts, to be able to get
  // real Riak node IPs later, by coverage plan request for proxying correct addresses
  protected override val riakHosts: Set[HostAndPort] = cluster.getIps
    .map(addr => HostAndPort.fromParts(addr, RiakNode.Builder.DEFAULT_REMOTE_PORT)).toSet

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
}

object FailoverTest {
  final val NODES_AMOUNT = 3
  final val STUBS_AMOUNT = 1

  @ClassRule
  def cluster: DockerRiakClusterRule = _cluster

  val _cluster: DockerRiakClusterRule = new DockerRiakClusterRule(
    new DockerRiakCluster(classOf[FailoverTest].getSimpleName, 3, 3))
}
