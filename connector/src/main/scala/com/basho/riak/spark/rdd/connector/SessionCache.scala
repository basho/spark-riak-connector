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
import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.concurrent.atomic.AtomicLong

import com.basho.riak.client.api.{RiakClient, RiakCommand}
import com.basho.riak.client.core.{FutureOperation, RiakCluster, RiakFuture, RiakNode}
import com.basho.riak.client.core.util.HostAndPort
import com.google.common.cache._

import scala.collection.JavaConverters._
import java.util.concurrent.{Executors, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.collection.JavaConversions._

import com.basho.riak.client.core.operations.ts.DescribeTableOperation
import com.basho.riak.spark.rdd.TsTimestampBindingType
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.spark.riak.Logging
import org.apache.spark.riak.types.RiakStructType
import org.apache.spark.sql.types.StructType

import scala.collection.concurrent.TrieMap
import scala.util.Try

/**
  * Simple [[RiakSession]] Cache/Pool.
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
class SessionCache(val cacheSize: Int) extends Logging {

  // Sessions to be released after timeout
  private val expiredSessions = TrieMap[CachedSession, Long]()

  private[this] val cachedSessions: LoadingCache[RiakConnectorConf, CachedSession] = {
    val cacheLoader = new CacheLoader[RiakConnectorConf, CachedSession]() {
      override def load(conf: RiakConnectorConf): CachedSession = {
        logDebug(s"Creating new Riak CachedSession: $conf")

        new CachedSession(conf, createClient(conf), release(_, conf.inactivityTimeout))

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
      new SessionProxy(session)).asInstanceOf[RiakSession]
  }

  def shutdown(): Unit = {
    checkForExpiredSessionsThread.interrupt()
    invalidateAll()
  }

  def invalidateAll(): Unit = {
    cachedSessions.invalidateAll()
  }

  def evict(key: RiakConnectorConf): Unit = {
    cachedSessions.invalidate(key)
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

      // To prevent 'stuck on exit' all the Java Client threads should be a non daemon/background
      val cluster = RiakCluster.builder(ns.asJava)
          .withBootstrap(new Bootstrap()
            .group(new NioEventLoopGroup(0, new DefaultThreadFactory(classOf[NioEventLoopGroup], true)))
            .channel(classOf[NioSocketChannel]))
          .withExecutor(new ScheduledThreadPoolExecutor(2, new ThreadFactory() {
            override def newThread(r: Runnable): Thread = {
              val t: Thread = Executors.defaultThreadFactory.newThread(r)
              t.setDaemon(true)
              t
            }
          }))
        .build()
      cluster.start()

      new RiakClient(cluster)
    } catch {
      case e: Throwable =>
        throw new IOException(
          s"Failed to create RiakClient(java) for $endpointsStr", e)
    }
  }

  private def releaseDeferred(session: CachedSession, releaseDelayMillis: Long): Unit = {
    val newTime = System.currentTimeMillis + releaseDelayMillis
    val releaseTime = expiredSessions.remove(session) match {
      case Some(oldTime) => math.max(oldTime, newTime)
      case None          => newTime
    }
    expiredSessions.put(session, releaseTime)
  }

  private def releaseImmediately(session: CachedSession): Unit = {
    if (session.getRefCount == 1) {
      evict(session.conf)
      logTrace(s"Session was released by timeout: ${session.conf}")
    }
  }

  def release(session: CachedSession, timeout: Long = 0): Unit = {
    if (timeout == 0 || !checkForExpiredSessionsThread.isAlive) {
      releaseImmediately(session)
    } else {
      releaseDeferred(session, timeout)
    }
  }

  private val checkForExpiredSessionsThread: Thread = new Thread("session-cleaner") {
    private val checkRate = 100

    setDaemon(true)

    override def run(): Unit = {
      logDebug(s"Started checking for expired sessions")
      try {
        while (!isInterrupted) {
          val now = System.currentTimeMillis
          for ((session, time) <- expiredSessions) {
            if (time <= now && expiredSessions.remove(session).isDefined) {
              try {
                releaseImmediately(session)
              } catch {
                case e: Exception => logError(s"Unexpected exception while releasing expired session: ${session.conf}", e)
              }
            }
          }
          Thread.sleep(checkRate)
        }
      } catch {
        case e: InterruptedException => {
          logTrace(s"Session Cleaner was interrupted", e)
        }
      }
      logDebug(s"Checking for expired sessions stopped")
    }
  }
  checkForExpiredSessionsThread.start
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
        } catch {
          case e: InvocationTargetException => throw e.getCause
        }
    }
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
class CachedSession(val conf: RiakConnectorConf, riakClient: RiakClient, afterClose: CachedSession => Any) extends RiakSession with Logging {
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

  def getRefCount(): Long = {
    refCount.get
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
      case Seq(first: RiakNode, rest @ _*) => first.getMinConnections
      case _                               => throw new IllegalArgumentException("requirement failed: At least 1 node required to obtain minConnection info")
    }
  }

  override def getTableDefinition(name: String, bindingType: TsTimestampBindingType): StructType = {
      val describeOp = new DescribeTableOperation.Builder(name).build()
      val tableDef = Try(execute(describeOp).get()) getOrElse {
        throw new IllegalStateException(s"No table $name was found")
      }

      RiakStructType(tableDef.getFullColumnDescriptions.toSeq, bindingType)
  }

  override def maxConnectionsPerNode: Int = {
    checkThatSessionIsActive()
    riakClient.getRiakCluster.getNodes.asScala match {
      case Seq(first: RiakNode, rest @ _*) => first.getMaxConnections
      case _                               => throw new IllegalArgumentException("requirement failed: At least 1 node required to obtain maxConnection info")
    }
  }

  override def close(): Unit = {
    checkThatSessionIsActive()
    refCount.decrementAndGet() match {
      case 0 =>
        unwrap().shutdown()
        logDebug(s"Riak CachedSession was destroyed/shutdown: $conf")
        closed = true
      case 1 =>
        afterClose(this)
      case _ => // Nothing to do
    }
  }

  override def isClosed: Boolean = closed
}

