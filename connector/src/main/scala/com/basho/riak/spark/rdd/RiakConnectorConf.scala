/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.rdd

import java.net.InetAddress

import com.basho.riak.client.core.util.HostAndPort
import org.apache.spark.{Logging, SparkConf}
import scala.util.control.NonFatal
import scala.collection.JavaConversions._


/** Stores configuration of a connection to Riak.
  */
case class RiakConnectorConf(
  hosts: Set[HostAndPort],
  minConnections: Int,
  maxConnections: Int)

object RiakConnectorConf extends Logging {
  val DEFAULT_MIN_CONNECTIONS = 20
  val DEFAULT_MAX_CONNECTIONS = 50
  val RiakConnectionHostProperty = "spark.riak.connection.host"
  val RiakMinConnectionPerHostProperty = "spark.riak.connections.min"
  val RiakMaxConnectionPerHostProperty = "spark.riak.connections.max"

  private def resolveHost(hostName: String): Option[HostAndPort] = {
    try Some(HostAndPort.fromString(hostName, 8087))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  def apply(conf: SparkConf): RiakConnectorConf = {
    val minConnections = conf.get(RiakMinConnectionPerHostProperty, DEFAULT_MIN_CONNECTIONS.toString).toInt
    val maxConnections = conf.get(RiakMaxConnectionPerHostProperty, DEFAULT_MAX_CONNECTIONS.toString).toInt
    val hostsStr = conf.get(RiakConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val hosts = HostAndPort.hostsFromString(hostsStr, 8087).toSet

    RiakConnectorConf(hosts, minConnections, maxConnections)
  }
}
