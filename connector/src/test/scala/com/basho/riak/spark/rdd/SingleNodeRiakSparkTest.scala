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

  protected override def riakHosts: Set[HostAndPort] =  HostAndPort.hostsFromString(
    dockerCluster.enabled() match {
      case true => dockerCluster.getIps.mkString(",")
      case _ => System.getProperty(RIAK_PBCHOST_PROPERTY, RiakNode.Builder.DEFAULT_REMOTE_ADDRESS)
      }, RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet
}

object SingleNodeRiakSparkTest {
  val RIAK_PBCHOST_PROPERTY = "com.basho.riak.pbchost"

  @ClassRule
  def dockerCluster: DockerRiakClusterRule = _dockerCluster

  val _dockerCluster: DockerRiakClusterRule = new DockerRiakClusterRule(DockerRiakCluster.builder()
    .withNodes(1)
    .withTimeout(1),
    System.getProperties.containsKey(RIAK_PBCHOST_PROPERTY))
}