/**
  * Copyright (c) 2016 Basho Technologies, Inc.
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

import java.io.IOException
import java.lang.reflect.{InvocationTargetException, Method, InvocationHandler, Proxy}
import java.util.concurrent.atomic.AtomicLong

import com.basho.riak.client.api.{RiakCommand, RiakClient}
import com.basho.riak.client.core.{RiakFuture, FutureOperation, RiakCluster, RiakNode}
import com.basho.riak.client.core.util.HostAndPort
import com.google.common.cache._
import org.apache.spark.Logging
import scala.collection.JavaConverters._


/**
  * Simple [[RiakSession]] Cache/Pool.
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
class SessionCache(val cacheSize: Int) extends Logging {

  private[this] val cachedSessions: LoadingCache[RiakConnectorConf, CachedSession] = {
    val cacheLoader = new CacheLoader[RiakConnectorConf, CachedSession]() {
      override def load(conf: RiakConnectorConf): CachedSession = {
        logDebug(s"Creating new Riak CachedSession: $conf")

        new CachedSession(conf, createClient(conf))

          /**
            * Additional reference prevents from closing after the first release:
            *
            * refCount == 1 means that session was returned to the cache/pool
            */
          .addRef()
      }
    }
    CacheBuilder.newBuilder()
      .maximumSize(cacheSize)
      .removalListener(new RemovalListener[RiakConnectorConf, CachedSession] {
        override def onRemoval(notification: RemovalNotification[RiakConnectorConf, CachedSession]): Unit = {
          val (conf, session) = notification.getKey -> notification.getValue

          logDebug(s"Riak CachedSession was evicted: $conf")
          session.close()
        }
      })
      .build(cacheLoader)
  }

  def get(conf: RiakConnectorConf): RiakSession = {
    wrap(cachedSessions.get(conf).addRef())
  }

  private def wrap(session: CachedSession): RiakSession = {
    Proxy.newProxyInstance(
      session.getClass.getClassLoader,
      Array(classOf[RiakSession]),
      new SessionProxy(session)
    ).asInstanceOf[RiakSession]
  }

  def shutdown(): Unit = {
    invalidateAll()
  }

  def invalidateAll(): Unit = {
    cachedSessions.invalidateAll()
  }

  private def createClient(conf: RiakConnectorConf): RiakClient = {
    lazy val addresses = conf.hosts.map(_.toString)
    lazy val endpointsStr = addresses.mkString("", ", ", "")

    try {
      logDebug(s"Attempting to create RiakClient(java) for $addresses")
      val builder = new RiakNode.Builder()
        .withMinConnections(conf.minConnections)
        .withMaxConnections(conf.maxConnections)


      val nodes = conf.hosts.map { (h: HostAndPort) =>
        builder.withRemoteAddress(h.getHost)
        builder.withRemotePort(h.getPort)
        builder.build()
      }

      val ns = nodes.toBuffer
      val cluster = RiakCluster.builder(ns.asJava).build()
      cluster.start()

      new RiakClient(cluster)
    } catch {
      case e: Throwable =>
        throw new IOException(
          s"Failed to create RiakClient(java) for $endpointsStr", e)
    }
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
private class SessionProxy(session: CachedSession) extends InvocationHandler {
  private var closed: Boolean = false

  override def invoke(proxy: scala.Any, method: Method, args: Array[AnyRef]): AnyRef = {
    (method.getName, method.getParameterTypes) match {
      case ("isClosed", Array()) =>
        closed.asInstanceOf[AnyRef]
      case _ =>
        try {
          require(!closed, "RiakSession should be active")
          if ("close".equals(method.getName)) {
            closed = true
          }
          method.invoke(session, args: _*)
        }
        catch {
          case e: InvocationTargetException =>
            throw e.getCause
        }
    }
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
class CachedSession(conf: RiakConnectorConf, riakClient: RiakClient) extends RiakSession with Logging {
  private var closed: Boolean = false
  private val refCount: AtomicLong = new AtomicLong(0)

  private def checkThatSessionIsActive(): Unit = {
    require(!isClosed, "CachedSession should be active")
  }

  override def unwrap(): RiakClient = {
    checkThatSessionIsActive()
    riakClient
  }

  def addRef(): CachedSession = {
    checkThatSessionIsActive()
    refCount.incrementAndGet()
    this
  }

  override def execute[T, S](command: RiakCommand[T, S]): T = {
    checkThatSessionIsActive()
    riakClient.execute(command)
  }

  override def execute[V, S](operation: FutureOperation[V, _, S]): RiakFuture[V, S] = {
    checkThatSessionIsActive()
    riakClient.getRiakCluster.execute(operation)
  }

  override def minConnectionsPerNode: Int = {
    checkThatSessionIsActive()
    riakClient.getRiakCluster.getNodes.asScala match {
      case Seq(first: RiakNode, rest@_ *) => first.getMinConnections
      case _ => throw new IllegalArgumentException("requirement failed: At least 1 node required to obtain minConnection info")
    }
  }

  override def maxConnectionsPerNode: Int = {
    checkThatSessionIsActive()
    riakClient.getRiakCluster.getNodes.asScala match {
      case Seq(first: RiakNode, rest@_ *) => first.getMaxConnections
      case _ => throw new IllegalArgumentException("requirement failed: At least 1 node required to obtain maxConnection info")
    }
  }

  override def close(): Unit = {
    checkThatSessionIsActive()
    if (0 == refCount.decrementAndGet()) {
      unwrap().shutdown()
      logDebug(s"Riak CachedSession was destroyed/shutdown: $conf")
      closed = true
    }
  }

  override def isClosed: Boolean = closed
}
