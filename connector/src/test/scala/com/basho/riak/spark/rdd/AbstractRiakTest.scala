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

import com.basho.riak.JsonTestFunctions
import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.query.Namespace
import org.junit._
import org.junit.rules.TestWatcher
import org.junit.runner.Description
import org.slf4j.{Logger, LoggerFactory}

abstract class AbstractRiakTest extends RiakFunctions with JsonTestFunctions {

  private final val logger: Logger = LoggerFactory.getLogger(this.getClass)

  protected val DEFAULT_NAMESPACE = new Namespace("default","test-bucket")
  protected val DEFAULT_NAMESPACE_4STORE = new Namespace("default", "test-bucket-4store")

  protected override val numberOfParallelRequests: Int = 4
  protected override val nodeBuilder: RiakNode.Builder = new RiakNode.Builder().withMinConnections(numberOfParallelRequests)

  protected val jsonData: Option[String] = None

  @Rule
  def watchman: TestWatcher = new TestWatcher() {
    override def starting(description: Description): Unit = {
      super.starting(description)
      logger.info(
        "\n----------------------------------------\n" +
          "  [TEST STARTED]  {}\n" +
          "----------------------------------------\n",
        description.getDisplayName)
    }

    override def finished(description: Description): Unit = {
      super.finished(description)
      logger.info(
        "\n----------------------------------------\n" +
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
    List(DEFAULT_NAMESPACE, DEFAULT_NAMESPACE_4STORE) foreach resetAndEmptyBucket

    withRiakDo(session => jsonData.foreach(createValues(session, DEFAULT_NAMESPACE, _)))
  }
}

