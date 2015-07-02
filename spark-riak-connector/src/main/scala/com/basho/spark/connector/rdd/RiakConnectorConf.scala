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
