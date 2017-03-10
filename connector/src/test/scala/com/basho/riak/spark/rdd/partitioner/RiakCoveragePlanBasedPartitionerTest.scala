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
import org.junit.{Ignore, Test}
import org.junit.Assert.assertEquals

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

    assertEquals(ReadConf.smartSplitMultiplier * defaultNumberOfSparkExecutors, partitions.length)

    assertEqualsUsingJSONIgnoreOrder("""[
        | {index: '${json-unit.ignore}', primaryHost: 'h4:0', queryData: {entries:[
        |     {host: 'h4:0', description: '15 -> 16'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h3:0', queryData: {entries:[
        |     {host: 'h3:0', description: '11 -> 12'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h1:0', queryData: {entries:[
        |     {host: 'h1:0', description: '1 -> 2'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h3:0', queryData: {entries:[
        |     {host: 'h3:0', description: '13 -> 14'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h2:0', queryData: {entries:[
        |     {host: 'h2:0', description: '7 -> 8'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h1:0', queryData: {entries:[
        |     {host: 'h1:0', description: '3 -> 4'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: "h3:0", queryData: {entries:[
        |     {host: 'h3:0', description: '9 -> 10'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h4:0', queryData: {entries:[
        |     {host: 'h4:0', description: '17 -> 18'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h2:0', queryData:{entries:[
        |     {host: 'h2:0', description: '5 -> 6'}]}}
        |
      ]""".stripMargin,
      partitions
    )
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
    assertEqualsUsingJSONIgnoreOrder("""[
        | {index: '${json-unit.ignore}', primaryHost: 'h2:0', queryData:{entries:[
        |     {host: 'h2:0', description: '5 -> 6'},
        |     {host: 'h2:0', description: '7 -> 8'}]}},
        | {index: '${json-unit.ignore}', primaryHost: 'h3:0', queryData:{entries:[
        |     {host: 'h3:0', description: '11 -> 12'},
        |     {host: 'h3:0', description: '13 -> 14'}]}},
        | {index: '${json-unit.ignore}', primaryHost: 'h4:0', queryData:{entries:[
        |     {host: 'h4:0', description: '15 -> 16'},
        |     {host: 'h4:0', description: '17 -> 18'}]}},
        | {index:'${json-unit.ignore}', primaryHost: 'h3:0', queryData:{entries:[
        |     {host: 'h3:0', description: '9 -> 10'}]}},
        | {index:'${json-unit.ignore}', primaryHost: 'h1:0', queryData:{entries:[
        |     {host: 'h1:0', description: '1 -> 2'},
        |     {host: 'h1:0', description: '3 -> 4'}]}}]
        |]""".stripMargin, partitions)
  }
}