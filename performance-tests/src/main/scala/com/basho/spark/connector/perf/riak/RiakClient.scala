package com.basho.spark.connector.perf.riak

import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.rdd.RiakFunctions

/**
 * @author anekhaev
 */
class RiakClient(
                  val riakHost: String,
                  val riakPort: Int,
                  override val numberOfParallelRequests: Int = 4) extends RiakFunctions {

  logger.info(s"Creating Riak client on $riakHost:$riakPort...")

  protected override val riakHosts: Set[HostAndPort] =
    Set(HostAndPort.fromParts(riakHost, riakPort))

  protected override val nodeBuilder: RiakNode.Builder = new RiakNode.Builder()
    .withMinConnections(numberOfParallelRequests)
    .withRemoteAddress(riakHost)
    .withRemotePort(riakPort)

}