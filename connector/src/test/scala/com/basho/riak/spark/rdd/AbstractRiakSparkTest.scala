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
package com.basho.riak.spark.rdd

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.query.{Location, Namespace, RiakObject}
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.test.cluster.DockerRiakCluster
import com.basho.riak.test.rule.DockerRiakClusterRule
import org.apache.spark.SparkContext
import org.junit.After

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import com.basho.riak.spark.rdd.AbstractRiakSparkTest._
import com.basho.riak.spark.rdd.mapper.ReadValueDataMapper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.ClassRule

import scala.collection.JavaConversions._


abstract class AbstractRiakSparkTest extends AbstractRiakTest {
  // SparkContext, created per test case
  protected val sparkSession: SparkSession = createSparkSession(initSparkConf())
  protected var sc: SparkContext = _

  protected override def riakHosts: Set[HostAndPort] =  HostAndPort.hostsFromString(
    dockerCluster.enabled() match {
      case true => dockerCluster.getIps.mkString(",")
      case _ => System.getProperty(RIAK_PBCHOST_PROPERTY, RiakNode.Builder.DEFAULT_REMOTE_ADDRESS)
    }, RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet

  protected def initSparkConf(): SparkConf = new SparkConf(false)
    .setMaster("local[2]")
    .setAppName(getClass.getSimpleName)
    .set("spark.riak.write.replicas", "1")
    .set("spark.riak.input.fetch-size", "2")
    .set("spark.riak.connection.host", riakHosts.map(hp => s"${hp.getHost}:${hp.getPort}").mkString(","))

  override def initialize(): Unit = {
    super.initialize()
    sc = sparkSession.sparkContext
  }

  protected def createSparkSession(conf: SparkConf): SparkSession = SparkSession.builder().config(conf).getOrCreate()

  @After
  def destroySparkContext(): Unit = Option(sc).foreach(x => x.stop())

  protected def fetchAllFromBucket(ns: Namespace): List[(String, String)] = {
    val data = ListBuffer[(String, String)]()
    withRiakDo(session =>
      foreachKeyInBucket(session, ns, (client, l: Location) => {
        val v = readByLocation[String](client, l)
        data += l.getKeyAsString -> v
      })
    )
    data.toList
  }

  protected def readByLocation[T: ClassTag](riakSession: RiakClient, location: Location): T = {
    readByLocation(riakSession, location, (l: Location, ro: RiakObject) => ReadValueDataMapper.mapValue[T](l, ro))
  }

  protected def stringify = (s: Array[String]) => s.mkString("[", ",", "]")
}

object AbstractRiakSparkTest {
  val RIAK_PBCHOST_PROPERTY = "com.basho.riak.pbchost"

  @ClassRule
  def dockerCluster: DockerRiakClusterRule = _dockerCluster

  val _dockerCluster: DockerRiakClusterRule = new DockerRiakClusterRule(DockerRiakCluster.builder()
    .withNodes(1)
    .withTimeout(2)
    .withForcePull(false),
    System.getProperties.containsKey(RIAK_PBCHOST_PROPERTY))
}
