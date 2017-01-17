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
package org.apache.spark.sql.riak

import scala.collection.JavaConversions.asScalaBuffer
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.rdd.connector.{RiakConnector, RiakConnectorConf}
import org.apache.spark.SparkConf

class OptionsTest {
  private val source = new DefaultSource

  private val initialHost = "default:1111"
  private val initialConnectionsMin = 111
  private val initialConnectionsMax = 999
  private val initialWquorum = 3
  private val initialFetchSize = 99
  private val initialSplitCount = 88

  private val hostAndPorts = toHostAndPorts(initialHost)

  protected def initSparkConf: SparkConf = new SparkConf(false)
    .setMaster("local")
    .setAppName(getClass.getSimpleName)
    // setting initial properties
    .set("spark.riak.connection.host", initialHost)
    .set("spark.riak.connections.min", initialConnectionsMin.toString)
    .set("spark.riak.connections.max", initialConnectionsMax.toString)
    .set("spark.riak.write.replicas", initialWquorum.toString)
    .set("spark.riak.input.fetch-size", initialFetchSize.toString)
    .set("spark.riak.input.split.count", initialSplitCount.toString)

  protected def createSparkSession(conf: SparkConf): SparkSession = SparkSession.builder().config(conf).getOrCreate()

  private val dummySchema = StructType(List(StructField("dummy", StringType, nullable = true)))
  private val sparkSession: SparkSession = createSparkSession(initSparkConf)
  private var df: DataFrame = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], dummySchema)

  @After
  def destroySparkContext(): Unit = {
    Option(sparkSession).foreach(sqlc => sqlc.sparkContext.stop())
  }

  @Test
  def noReadOptionsShouldResultInKeepingInitialProperties(): Unit = {
    val rel = source.createRelation(sparkSession.sqlContext,
      Map("path" -> "path"), dummySchema).asInstanceOf[RiakRelation]
    val riakConnector = getConnector(rel)
    val riakConf = getRiakConnectorConf(riakConnector)
    val readConf = rel.readConf
    assertEquals(initialFetchSize, readConf.fetchSize)
    assertEquals(initialSplitCount, readConf.splitCount)
    assertEquals(hostAndPorts, riakConf.hosts)
    assertEquals(initialConnectionsMin, riakConf.minConnections)
    assertEquals(initialConnectionsMax, riakConf.maxConnections)
  }

  @Test
  def noWriteOptionsShouldResultInKeepingInitialProperties(): Unit = {
    val rel = source.createRelation(sparkSession.sqlContext, SaveMode.Append,
      Map("path" -> "path"), df).asInstanceOf[RiakRelation]
    val riakConnector = getConnector(rel)
    val writeConf = rel.writeConf
    val riakConf = getRiakConnectorConf(riakConnector)
    assertEquals(initialWquorum.toString, writeConf.writeReplicas)

    assertEquals(hostAndPorts, riakConf.hosts)
    assertEquals(initialConnectionsMin, riakConf.minConnections)
    assertEquals(initialConnectionsMax, riakConf.maxConnections)
  }

  @Test
  def writeOptionsOnReadShouldNotAffectProperties(): Unit = {
    val newQuorum = 1
    val rel = source.createRelation(sparkSession.sqlContext,
      Map("path" -> "path", "spark.riak.write.replicas" -> newQuorum.toString), dummySchema).asInstanceOf[RiakRelation]
    val writeConf = rel.writeConf
  }

  @Test
  def readOptionsOnWriteShouldNotAffectProperties(): Unit = {
    val newFetchSize = 100
    val newSplitCount = 10
    val rel = source.createRelation(sparkSession.sqlContext, SaveMode.Append,
      Map("path" -> "path", "spark.riak.input.fetch-size" -> newFetchSize.toString,
        "spark.riak.input.split.count" -> newSplitCount.toString), df).asInstanceOf[RiakRelation]
    val readConf = rel.readConf
  }

  @Test
  def writeOptionsOnWriteShouldAffectProperties(): Unit = {
    val newQuorum = 1
    val rel = source.createRelation(sparkSession.sqlContext, SaveMode.Append,
      Map("path" -> "path", "spark.riak.write.replicas" -> newQuorum.toString), df).asInstanceOf[RiakRelation]
    val writeConf = rel.writeConf
    assertEquals(newQuorum, writeConf.writeReplicas.toInt)
  }

  @Test
  def readOptionsOnReadShouldAffectProperties(): Unit = {
    val newFetchSize = 100
    val newSplitCount = 10
    val rel = source.createRelation(sparkSession.sqlContext, Map("path" -> "path", "spark.riak.input.fetch-size" -> newFetchSize.toString,
      "spark.riak.input.split.count" -> newSplitCount.toString), dummySchema).asInstanceOf[RiakRelation]
    val readConf = rel.readConf
    assertEquals(newFetchSize, readConf.fetchSize)
    assertEquals(newSplitCount, readConf.splitCount)
  }

  @Test
  def riakConnectionOptionsShouldAffectProperties(): Unit = {
    val newHost = "newHost:9999"
    val newConnectionsMin = 1
    val newConnectionsMax = 9
    val rel = source.createRelation(sparkSession.sqlContext,
      Map("path" -> "path", "spark.riak.connection.host" -> newHost,
        "spark.riak.connections.min" -> newConnectionsMin.toString,
        "spark.riak.connections.max" -> newConnectionsMax.toString), dummySchema).asInstanceOf[RiakRelation]
    val riakConnector = getConnector(rel)
    val riakConf = getRiakConnectorConf(riakConnector)
    assertEquals(toHostAndPorts(newHost), riakConf.hosts)
    assertEquals(newConnectionsMin, riakConf.minConnections)
    assertEquals(newConnectionsMax, riakConf.maxConnections)
  }

  @Test
  def riakConnectionOptionsShouldChangeOnlySpecifiedProperties(): Unit = {
    val newHost = "newHost:9999"
    val rel = source.createRelation(sparkSession.sqlContext,
      Map("path" -> "path", "spark.riak.connection.host" -> newHost), dummySchema).asInstanceOf[RiakRelation]
    val riakConnector = getConnector(rel)
    val riakConf = getRiakConnectorConf(riakConnector)
    assertEquals(toHostAndPorts(newHost), riakConf.hosts)
    assertEquals(initialConnectionsMin, riakConf.minConnections)
    assertEquals(initialConnectionsMax, riakConf.maxConnections)
  }

  private def getConnector(rel: RiakRelation): RiakConnector = {
    getPrivateField[RiakConnector, RiakRelation](rel, "connector")
  }

  private def getRiakConnectorConf(connector: RiakConnector): RiakConnectorConf = {
    getPrivateField[RiakConnectorConf, RiakConnector](connector, "_config")
  }

  private def getPrivateField[T, U](source: U, fieldName: String): T = {
    val field = source.getClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field.get(source).asInstanceOf[T]
  }

  private def toHostAndPorts(hostStr: String): Set[HostAndPort] = {
    HostAndPort.hostsFromString(hostStr, RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet
  }
}
