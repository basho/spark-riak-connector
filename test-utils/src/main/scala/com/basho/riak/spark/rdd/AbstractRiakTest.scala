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

import java.io.IOException

import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.util.HostAndPort
import com.fasterxml.jackson.core.JsonProcessingException
import net.javacrumbs.jsonunit.JsonAssert
import net.javacrumbs.jsonunit.core.{Configuration, Option}
import org.apache.commons.lang3.StringUtils
import org.junit.rules.TestWatcher
import org.junit.runner.Description
import org.junit.{Before, Rule}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

abstract class AbstractRiakTest extends RiakFunctions{

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  protected val DEFAULT_NAMESPACE = new Namespace("default","test-bucket")
  protected val DEFAULT_NAMESPACE_4STORE = new Namespace("default", "test-bucket-4store")
  protected val DEFAULT_RIAK_HOST = System.getProperty("com.basho.riak.pbchost", RiakNode.Builder.DEFAULT_REMOTE_ADDRESS)

  protected override val riakHosts:Set[HostAndPort] = HostAndPort.hostsFromString(DEFAULT_RIAK_HOST, RiakNode.Builder.DEFAULT_REMOTE_PORT).toSet
  protected override val numberOfParallelRequests: Int = 4

  protected override val nodeBuilder: RiakNode.Builder =
    new RiakNode.Builder()
      .withMinConnections(numberOfParallelRequests)

  protected def jsonData(): String = null

  @Rule
  def watchman = new TestWatcher() {
    override def starting(description: Description): Unit = {
      super.starting(description)
      logger.info("\n----------------------------------------\n" +
                  "  [TEST STARTED]  {}\n" +
          "----------------------------------------\n",
        description.getDisplayName)
    }

    override def finished(description: Description): Unit = {
      super.finished(description)
      logger.info("\n----------------------------------------\n" +
        "  [TEST FINISHED]  {}\n" +
        "----------------------------------------\n",
        description.getDisplayName)
    }
  }

  @Before
  protected def initialize(): Unit = setupData()

  protected def setupData(): Unit = {
    // Purge data: data might be not only created, but it may be also changed during the previous test case execution
    //
    // For manual check: curl -v http://localhost:10018/buckets/test-bucket/keys?keys=true
    List(DEFAULT_NAMESPACE, DEFAULT_NAMESPACE_4STORE) foreach (x => resetAndEmptyBucket(x))

    withRiakDo(session => {
      val data: String = jsonData()

      if (StringUtils.isNotBlank(data)) {
        createValues(session, DEFAULT_NAMESPACE, data)
      }
    })
  }

  protected def assertEqualsUsingJSON(jsonExpected: AnyRef, actual: AnyRef): Unit = {
    assertEqualsUsingJSONImpl(jsonExpected, actual, null)
  }

  protected def assertEqualsUsingJSONIgnoreOrder(jsonExpected: AnyRef, actual: AnyRef): Unit = {
    assertEqualsUsingJSONImpl(jsonExpected, actual, JsonAssert.when(Option.IGNORING_ARRAY_ORDER))
  }

  private def assertEqualsUsingJSONImpl(jsonExpected: AnyRef, actual: AnyRef, configuration: Configuration) {
    var strExpected: String = null
    var strActual: String = null
    try{
      strExpected = tolerantMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parseIfString(jsonExpected))
      strActual = tolerantMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parseIfString(actual))
    }catch{
      case ex: JsonProcessingException => throw new RuntimeException(ex)
    }

    if(configuration != null) {
      JsonAssert.assertJsonEquals(strExpected, strActual, configuration)
    } else {
      JsonAssert.assertJsonEquals(strExpected, strActual)
    }
  }
  
  private def parseIfString(raw: AnyRef): Object = {
    raw match {
      case str: String =>
        try {
          tolerantMapper.readValue(str, classOf[java.lang.Object])
        } catch {
          case ex: IOException => throw new RuntimeException(ex)
        }
      case _ => raw
    }
  }
}
