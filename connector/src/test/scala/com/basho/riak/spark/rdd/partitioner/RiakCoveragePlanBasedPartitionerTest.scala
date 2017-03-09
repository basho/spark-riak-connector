/**
  * Copyright (c) 2015-2017 Basho Technologies, Inc.
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
package com.basho.riak.spark.rdd.partitioner

import com.basho.riak.spark.rdd.{ReadConf, RiakRDD}
import org.junit.Test
import org.junit.Assert.assertEquals

/**
  * Created by srg on 2/28/17.
  */
class RiakCoveragePlanBasedPartitionerTest extends AbstractCoveragePlanBasedPartitionerTest {
  val defaultNumberOfSparkExecutors = 3

  @Test
  def smartSplitShouldBeUsedByDefault(): Unit = {
    mockKVCoveragePlan(
      ("h1", 1 -> 2),
      ("h1", 3 -> 4),
      ("h2", 5 -> 6),
      ("h2", 7 -> 8),
      ("h3", 9 -> 10),
      ("h3", 11 -> 12),
      ("h3", 13 -> 14),
      ("h4", 15 -> 16),
      ("h4", 17 -> 18)
    )

    mockSparkExecutorsNumber(defaultNumberOfSparkExecutors)
    val rdd = new RiakRDD(sc, rc, "default", "test").queryAll()
    val partitions = rdd.partitions

    // WEIRD but due to issue in partitioner there is only 8 partitions
    assertEquals(ReadConf.smartSplitMultiplier * defaultNumberOfSparkExecutors -1 , partitions.length)
  }

  @Test
  def explicitlySpecifiedSplitCountShouldOverrideDefaultOne(): Unit = {

    mockKVCoveragePlan(
      ("h1", 1 -> 2),
      ("h1", 3 -> 4),
      ("h2", 5 -> 6),
      ("h2", 7 -> 8),
      ("h3", 9 -> 10),
      ("h3", 11 -> 12),
      ("h3", 13 -> 14),
      ("h4", 15 -> 16),
      ("h4", 17 -> 18)
    )

    mockSparkExecutorsNumber(defaultNumberOfSparkExecutors)
    val requestedSplit = 5
    val rdd = new RiakRDD(sc, rc, "default", "test", None, ReadConf(_splitCount=Some(requestedSplit))).queryAll()
    val partitions = rdd.partitions

    assertEquals(requestedSplit, partitions.length)
  }
}