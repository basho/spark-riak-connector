package com.basho.riak

import com.basho.riak.test.cluster.DockerRiakCluster

package object cluster {

  def withDockerizedRiak(func: DockerRiakCluster => Unit): Unit = withDockerizedRiakCluster(1)(func)

  def withDockerizedRiakCluster(nodes: Int)(func: DockerRiakCluster => Unit): Unit = {
    val dockerCluster = DockerRiakCluster.builder()
      .withNodes(nodes)
      .withTimeout(nodes) // use one minute per one node
      .build()
    try {
      dockerCluster.start()
      func(dockerCluster)
    } finally {
      dockerCluster.stop()
    }
  }
}
