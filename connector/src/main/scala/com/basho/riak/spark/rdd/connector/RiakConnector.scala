/**
  * Copyright (c) 2015 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.basho.riak.spark.rdd.connector


import com.basho.riak.client.core.util.HostAndPort
import org.apache.spark.{Logging, SparkConf}

/**
  * Provides and manages [[RiakSession]].
  *
  * Can be either given explicitly or automatically configured from `SparkConf`.
  * The connection options are:
  * - `spark.riak.connection.hosts`: contact point to connect to the Riak cluster
  * - `spark.riak.connections.min`: minimum number of connections per one RiakNode
  * - `spark.riak.connections.max`: maximum number of connections per one RiakNode
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  */
class RiakConnector(conf: RiakConnectorConf)
  extends Serializable with Logging {

  private[this] val _config = conf

  /** Known cluster hosts. This is going to return all cluster hosts after at least one successful connection has been made */
  def hosts: Set[HostAndPort] = _config.hosts
  def minConnections = _config.minConnections

  def openSession(hosts: Option[Seq[HostAndPort]] = None): RiakSession = {
    val cfg = hosts match {
      case None =>
        _config

      case Some(h: Seq[HostAndPort]) =>
        RiakConnectorConf(hosts = Set(h: _*), minConnections = _config.minConnections,
          maxConnections = _config.maxConnections, inactivityTimeout = _config.inactivityTimeout)
    }

    RiakConnector.openSession(cfg)
  }

  def withSessionDo[T](hosts: Option[Seq[HostAndPort]] = None)(code: RiakSession => T): T =
    closeSessionAfterUse(openSession(hosts)) { session => code(session) }

  def withSessionDo[T](code: RiakSession => T): T = withSessionDo(None)(code)

  def closeSessionAfterUse[T](closeable: RiakSession)(code: RiakSession => T): T =
    try code(closeable) finally {
      closeable.close()
    }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
object RiakConnector extends Logging {
  private var sessionCache = new SessionCache(100)

  /**
    * @note was introduced for testing purposes only, it is not threadsafe
    */
  private[connector] def invalidateSessionCache(maxSize: Int = sessionCache.cacheSize): Unit = {
    sessionCache.shutdown()
    sessionCache = new SessionCache(maxSize)
  }

  private def openSession(conf: RiakConnectorConf): RiakSession =
    sessionCache.get(conf)

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run(): Unit = {
      sessionCache.shutdown()
    }
  }))

  /** Returns a RiakConnector created from properties found in the `SparkConf` object */
  def apply(conf: SparkConf): RiakConnector = {
    new RiakConnector(RiakConnectorConf(conf))
  }

  /** Returns a RiakConnector created from explicitly given connection configuration. */
  def apply(hosts: Set[HostAndPort], minConnections: Int = RiakConnectorConf.DEFAULT_MIN_CONNECTIONS,
            maxConnections: Int = RiakConnectorConf.DEFAULT_MAX_CONNECTIONS,
            inactivityTimeout: Long = RiakConnectorConf.defaultInactivityTimeout): RiakConnector = {

    val config = RiakConnectorConf(hosts, minConnections, maxConnections, inactivityTimeout)
    new RiakConnector(config)
  }
}
