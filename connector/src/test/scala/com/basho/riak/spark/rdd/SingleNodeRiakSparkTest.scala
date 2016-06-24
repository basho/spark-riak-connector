package com.basho.riak.spark.rdd

import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.rdd.SingleNodeRiakSparkTest._
import com.basho.riak.test.cluster.DockerRiakCluster
import com.basho.riak.test.rule.DockerRiakClusterRule
import org.apache.spark.SparkConf
import org.junit.ClassRule

import scala.collection.JavaConversions._

abstract class SingleNodeRiakSparkTest extends AbstractRiakSparkTest {

  protected override def riakHosts: Set[HostAndPort] =
    HostAndPort.hostsFromString(dockerCluster.getIps.mkString(","), RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet

  override protected def initSparkConf(): SparkConf = super.initSparkConf()
    .set("spark.riak.connection.host", dockerCluster.getIps.map(x => s"$x:${RiakNode.Builder.DEFAULT_REMOTE_PORT}").mkString(","))
}

object SingleNodeRiakSparkTest {

  @ClassRule
  def dockerCluster: DockerRiakClusterRule = _dockerCluster

  val _dockerCluster: DockerRiakClusterRule = new DockerRiakClusterRule(DockerRiakCluster.builder()
    .withNodes(1)
    .withTimeout(1))
}
