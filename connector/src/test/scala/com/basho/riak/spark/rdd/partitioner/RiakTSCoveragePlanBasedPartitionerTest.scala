/**
  * *****************************************************************************
  * Copyright (c) 2016 IBM Corp.
  *
  * Created by Basho Technologies for IBM
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * *****************************************************************************
  */
package com.basho.riak.spark.rdd.partitioner


import com.basho.riak.spark.rdd.{ReadConf, RegressionTests, RiakTSRDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, LessThan}
import org.junit.{Ignore, Test}
import org.junit.experimental.categories.Category

class RiakTSCoveragePlanBasedPartitionerTest extends AbstractCoveragePlanBasedPartitionerTest {

  val filters: Array[Filter] = Array(
    GreaterThanOrEqual("time", 0),
    LessThan("time", 1000))

  @Test
  @Category(Array(classOf[RegressionTests]))
  def checkPartitioningForIrregularData(): Unit = {

    // host -> range(from->to)
    mockTSCoveragePlan(
      ("h1", 1->2),
      ("h2", 3->4),
      ("h2", 5->6),
      ("h2", 7->8),
      ("h3", 11->12)
    )

    val partitioner = new RiakTSCoveragePlanBasedPartitioner(rc, "test", None, None, new Array[Filter](0), new ReadConf())
    val partitions = partitioner.partitions()
    assertEqualsUsingJSONIgnoreOrder(
      """[
        | {index: '${json-unit.ignore}', queryData: {primaryHost: 'h3:0', entry: '[11,12)@h3'}},
        |
        | {index: '${json-unit.ignore}', queryData:[
        |     {primaryHost: 'h2:0', entry: '[5,6)@h2'},
        |     {primaryHost: 'h2:0', entry: '[7,8)@h2'}]},
        |
        | {index: '${json-unit.ignore}', queryData: {primaryHost: 'h1:0', entry: '[1,2)@h1'}},
        |
        | {index: '${json-unit.ignore}', queryData: {primaryHost: 'h2:0', entry: '[3,4)@h2'}}
      ]""".stripMargin, partitions)
  }

  @Test
  @Category(Array(classOf[RegressionTests]))
  def checkPartitioningForRegullarData(): Unit = {

    // host -> range(from->to)
    mockTSCoveragePlan(
      ("1", 1 -> 2),
      ("1", 3 -> 4),
      ("2", 5 -> 6),
      ("2", 7 -> 8),
      ("3", 9 -> 10)
    )

    val partitioner = new RiakTSCoveragePlanBasedPartitioner(rc, "test", None, None, new Array[Filter](0), new ReadConf())
    val partitions = partitioner.partitions()


    assertEqualsUsingJSONIgnoreOrder(
      """[
        | {index: '${json-unit.ignore}', queryData: {primaryHost: '1:0', entry: '[1,2)@1'}},
        | {index: '${json-unit.ignore}', queryData: {primaryHost: '1:0', entry: '[3,4)@1'}},
        | {index: '${json-unit.ignore}', queryData: {primaryHost: '2:0', entry: '[5,6)@2'}},
        | {index: '${json-unit.ignore}', queryData: {primaryHost: '3:0', entry: '[9,10)@3'}},
        | {index: '${json-unit.ignore}', queryData: {primaryHost: '2:0', entry: '[7,8)@2'}}
      ]""".stripMargin, partitions)
  }

  @Test
  def coveragePlanBasedPartitioningLessThanSplitCount(): Unit = {
    mockTSCoveragePlan(
      ("h1", 1 -> 2),
      ("h2", 3 -> 4),
      ("h3", 5 -> 6)
    )

    val rdd = new RiakTSRDD[Row](sc, rc, "test", None, None, None, filters)
    val partitions = rdd.partitions
      assertEqualsUsingJSONIgnoreOrder(
        """[
          | {index: '${json-unit.ignore}', queryData: {primaryHost: 'h3:0', entry: '[5,6)@h3'}},
          | {index: '${json-unit.ignore}', queryData: {primaryHost: 'h1:0', entry: '[1,2)@h1'}},
          | {index: '${json-unit.ignore}', queryData: {primaryHost: 'h2:0', entry: '[3,4)@h2'}}
        ]""".stripMargin, partitions)
  }

  @Test
  def coveragePlanBasedPartitioningGreaterThanSplitCount(): Unit = {
    val requestedSplitCount = 3

    mockTSCoveragePlan(
      ("h1", 1 -> 2),
      ("h1", 3 -> 4),
      ("h1", 5 -> 6),
      ("h2", 6 -> 7),
      ("h2", 8 -> 9),
      ("h2", 10 -> 11),
      ("h2", 12 -> 13),
      ("h3", 14 -> 15),
      ("h3", 16 -> 17),
      ("h3", 18 -> 19)
    )
    val rdd = new RiakTSRDD[Row](sc, rc, "test", None, None, None, filters, readConf = ReadConf(_splitCount=Some(requestedSplitCount)))
    val partitions = rdd.partitions

    assertEqualsUsingJSONIgnoreOrder("""[
      | {index:'${json-unit.ignore}', queryData:[
      |     {primaryHost: 'h3:0', entry: '[14,15)@h3'},
      |     {primaryHost: 'h3:0', entry: '[16,17)@h3'},
      |     {primaryHost: 'h3:0', entry: '[18,19)@h3'}]},
      |
      | {index: '${json-unit.ignore}',queryData:[
      |     {primaryHost: 'h2:0', entry: '[6,7)@h2'},
      |     {primaryHost: 'h2:0', entry: '[8,9)@h2'},
      |     {primaryHost: 'h2:0', entry: '[10,11)@h2'},
      |     {primaryHost: 'h2:0', entry: '[12,13)@h2'}]},
      |
      | {index: '${json-unit.ignore}',queryData:[
      |     {primaryHost: 'h1:0', entry: '[1,2)@h1'},
      |     {primaryHost: 'h1:0', entry: '[3,4)@h1'},
      |     {primaryHost: 'h1:0', entry: '[5,6)@h1'}]}
      ]""".stripMargin, partitions)
  }
}