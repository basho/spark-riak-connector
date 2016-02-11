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
package com.basho.riak.spark.rdd.connector

import java.net.InetAddress
import com.basho.riak.client.core.util.HostAndPort
import org.apache.spark.{Logging, SparkConf}
import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import com.basho.riak.client.core.RiakNode


/** Stores configuration of a connection to Riak.
  */
case class RiakConnectorConf(
  hosts: Set[HostAndPort],
  minConnections: Int,
  maxConnections: Int,
  inactivityTimeout: Long) {
  
  def overrideProperties(options: Map[String, String]): RiakConnectorConf = {    
    val newMinConnections = options.getOrElse(RiakConnectorConf.RiakMinConnectionPerHostProperty, minConnections.toString).toInt
    val newMaxConnections = options.getOrElse(RiakConnectorConf.RiakMaxConnectionPerHostProperty, maxConnections.toString).toInt 
    val newInactivityTimeout = options.getOrElse(RiakConnectorConf.RiakInactivityTimeoutProperty, inactivityTimeout.toString).toLong 
    val hostStr = options.get(RiakConnectorConf.RiakConnectionHostProperty)
    val newHosts = hostStr match {
      case Some(address) => HostAndPort.hostsFromString(address, RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet
      case None => hosts
    }
    RiakConnectorConf(newHosts, newMinConnections, newMaxConnections, newInactivityTimeout)
  }
}

object RiakConnectorConf extends Logging {
  val DEFAULT_MIN_CONNECTIONS = 20
  val DEFAULT_MAX_CONNECTIONS = 50
  val defaultInactivityTimeout = 1000
  val RiakConnectionHostProperty = "spark.riak.connection.host"
  val RiakMinConnectionPerHostProperty = "spark.riak.connections.min"
  val RiakMaxConnectionPerHostProperty = "spark.riak.connections.max"
  val RiakInactivityTimeoutProperty = "spark.riak.connections.inactivity.timeout"

  private def resolveHost(hostName: String): Option[HostAndPort] = {
    try Some(HostAndPort.fromString(hostName, RiakNode.Builder.DEFAULT_REMOTE_PORT))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  /** Creates RiakConnectorConf based on properties provided to Spark Conf
  *
  * @param conf SparkConf of Spark context with Riak-related properties
  */
  def apply(conf: SparkConf): RiakConnectorConf = {
    val minConnections = conf.get(RiakMinConnectionPerHostProperty, DEFAULT_MIN_CONNECTIONS.toString).toInt
    val maxConnections = conf.get(RiakMaxConnectionPerHostProperty, DEFAULT_MAX_CONNECTIONS.toString).toInt
    val inactivityTimeout = conf.get(RiakInactivityTimeoutProperty, defaultInactivityTimeout.toString).toLong
    val hostsStr = conf.get(RiakConnectionHostProperty, InetAddress.getLocalHost.getHostAddress)
    val hosts = HostAndPort.hostsFromString(hostsStr, RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet

    RiakConnectorConf(hosts, minConnections, maxConnections, inactivityTimeout)
  }
  
  
  /** Creates RiakConnectorConf based on an externally provided map of properties 
  *   to override those of SparkCon 
  *
  * @param conf SparkConf of Spark context to be taken as defaults
  * @param options externally provided map of properties 
  */
  def apply(conf: SparkConf, options: Map[String, String]): RiakConnectorConf = {
    val riakConf = RiakConnectorConf(conf)
    riakConf.overrideProperties(options)
  }
}
