package com.basho.riak.spark.rdd.connector

import com.basho.riak.client.core.util.HostAndPort
import org.junit.{Test, Before}
import scala.collection.JavaConversions._
import org.junit.Assert._

// TODO: Consider to use Mocks
class RiakConnectorTests {
  private val conf_1 = RiakConnectorConf(HostAndPort.hostsFromString("1.1.1.1", 8087).toSet, 1, 2)
  private val connector_1 = new RiakConnector(conf_1)

  @Before
  def purgeSessionCache(): Unit = {
    RiakConnector.invalidateSessionCache()
  }

  @Test
  def openSessionWithDefaultParametersShouldReturnSessionFromPool(): Unit = {
    val session_1 = connector_1.openSession()
    val client_1 = session_1.unwrap()

    session_1.close()

    val session_2 = connector_1.openSession()
    val client_2 = session_2.unwrap()

    assertTrue(client_1 eq client_2)
  }

  @Test
  def openSessionWithHostsShouldReturnSessionFromPool(): Unit = {
    val hosts = HostAndPort.hostsFromString("2.2.2.2", 8780).toSeq

    val session_1 = connector_1.openSession(Some(hosts))
    val client_1 = session_1.unwrap()

    session_1.close()

    val session_2 = connector_1.openSession(Some(hosts))
    val client_2 = session_2.unwrap()

    assertTrue(client_1 eq client_2)
  }
}
