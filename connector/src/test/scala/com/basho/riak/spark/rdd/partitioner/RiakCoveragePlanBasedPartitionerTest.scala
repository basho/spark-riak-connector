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
      ("h1", 1),
      ("h1", 2),
      ("h2", 3),
      ("h2", 4),
      ("h3", 5),
      ("h3", 6),
      ("h3", 7),
      ("h4", 8),
      ("h4", 9)
    )

    mockSparkExecutorsNumber(defaultNumberOfSparkExecutors)
    val rdd = new RiakRDD(sc, rc, "default", "test").queryAll()
    val partitions = rdd.partitions

    assertEquals(ReadConf.smartSplitMultiplier * defaultNumberOfSparkExecutors, partitions.length)

    assertEqualsUsingJSONIgnoreOrder("""[
        | {index: '${json-unit.ignore}', primaryHost: 'h4:0', queryData: {entries:[
        |     {host: 'h4:0', description: '8'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h3:0', queryData: {entries:[
        |     {host: 'h3:0', description: '6'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h1:0', queryData: {entries:[
        |     {host: 'h1:0', description: '1'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h3:0', queryData: {entries:[
        |     {host: 'h3:0', description: '7'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h2:0', queryData: {entries:[
        |     {host: 'h2:0', description: '4'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h1:0', queryData: {entries:[
        |     {host: 'h1:0', description: '2'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: "h3:0", queryData: {entries:[
        |     {host: 'h3:0', description: '5'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h4:0', queryData: {entries:[
        |     {host: 'h4:0', description: '9'}]}},
        |
        | {index: '${json-unit.ignore}', primaryHost: 'h2:0', queryData:{entries:[
        |     {host: 'h2:0', description: '3'}]}}
        |
      ]""".stripMargin,
      partitions
    )
  }

  @Test
  def explicitlySpecifiedSplitCountShouldOverrideDefaultOne(): Unit = {

    mockKVCoveragePlan(
      ("h1", 1),
      ("h1", 2),
      ("h2", 3),
      ("h2", 4),
      ("h3", 5),
      ("h3", 6),
      ("h3", 7),
      ("h4", 8),
      ("h4", 9)
    )

    mockSparkExecutorsNumber(defaultNumberOfSparkExecutors)
    val requestedSplit = 5
    val rdd = new RiakRDD(sc, rc, "default", "test", None, ReadConf(_splitCount=Some(requestedSplit))).queryAll()
    val partitions = rdd.partitions

    assertEquals(requestedSplit, partitions.length)
    assertEqualsUsingJSONIgnoreOrder("""[
        | {index: '${json-unit.ignore}', primaryHost: 'h2:0', queryData:{entries:[
        |     {host: 'h2:0', description: '3'},
        |     {host: 'h2:0', description: '4'}]}},
        | {index: '${json-unit.ignore}', primaryHost: 'h3:0', queryData:{entries:[
        |     {host: 'h3:0', description: '6'},
        |     {host: 'h3:0', description: '7'}]}},
        | {index: '${json-unit.ignore}', primaryHost: 'h4:0', queryData:{entries:[
        |     {host: 'h4:0', description: '8'},
        |     {host: 'h4:0', description: '9'}]}},
        | {index:'${json-unit.ignore}', primaryHost: 'h3:0', queryData:{entries:[
        |     {host: 'h3:0', description: '5'}]}},
        | {index:'${json-unit.ignore}', primaryHost: 'h1:0', queryData:{entries:[
        |     {host: 'h1:0', description: '1'},
        |     {host: 'h1:0', description: '2'}]}}]
        |]""".stripMargin, partitions)
  }
}