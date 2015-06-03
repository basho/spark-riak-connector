package com.basho.spark.connector.rdd

import java.net.InetAddress

import com.google.common.net.HostAndPort
import org.apache.spark.{Logging, SparkConf}
import scala.util.control.NonFatal


/** Stores configuration of a connection to Riak.
  */
case class RiakConnectorConf(
  hosts: Set[HostAndPort])

object RiakConnectorConf extends Logging {
  val RiakConnectionHostProperty = "spark.riak.connection.host"

  private def resolveHost(hostName: String): Option[HostAndPort] = {
    try Some(HostAndPort.fromString(hostName))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  def apply(conf: SparkConf): RiakConnectorConf = {
    val hostsStr = conf.get(RiakConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName.trim)
    } yield hostAddress

    RiakConnectorConf(hosts)
  }
}
