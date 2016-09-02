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


import com.basho.riak.JsonTestFunctions
import com.basho.riak.client.api.commands.timeseries.CoveragePlan
import com.basho.riak.client.core.query.timeseries.{CoverageEntry, CoveragePlanResult}
import com.basho.riak.spark.rdd.{ReadConf, RegressionTests, RiakTSRDD}
import com.basho.riak.spark.rdd.connector.{RiakConnector, RiakSession}
import com.fasterxml.jackson.core.{JsonGenerator, Version}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, LessThan}
import org.junit.{Before, Test}
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConversions._

@RunWith(classOf[MockitoJUnitRunner])
class RiakTSCoveragePlanBasedPartitionerTest extends JsonTestFunctions {

  @Mock
  private val rc: RiakConnector = null

  @Mock
  private val rs: RiakSession = null

  @Mock
  private val sc: SparkContext = null

  // To access protected constructor in CoveragePlanResult
  class SimpleCoveragePlanResult extends CoveragePlanResult {
  }

  val filters: Array[Filter] = Array(
    GreaterThanOrEqual("time", 0),
    LessThan("time", 1000))

  private var coveragePlan: SimpleCoveragePlanResult = null

  override protected def tolerantMapper: ObjectMapper = super.tolerantMapper
    .registerModule(
      new SimpleModule("RiakTs2 Module", new Version(1, 0, 0, null))
        .addSerializer(classOf[RiakTSPartition], new RiakTSPartitionSerializer)
          .addSerializer(classOf[CoverageEntry], new RiakCoverageEntrySerializer))

  @Before
  def initializeMocks(): Unit = {
    doAnswer(new Answer[CoveragePlanResult] {
      override def answer(invocation: InvocationOnMock): CoveragePlanResult = coveragePlan
    }).when(rs).execute(any[CoveragePlan])

    doAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val func = invocation.getArguments()(0).asInstanceOf[RiakSession => AnyRef]
        func.apply(rs)
      }
    }).when(rc).withSessionDo(any(classOf[Function1[RiakSession, CoveragePlanResult]]))

    coveragePlan = new SimpleCoveragePlanResult
  }

  @Test
  @Category(Array(classOf[RegressionTests]))
  def checkPartitioningForIrregularData1(): Unit = {

    // host -> range(from->to)
    makeCoveragePlan(
      ("1", 1->2),
      ("2", 3->4),
      ("2", 5->6),
      ("2", 7->8),
      ("3", 9->10)
    )

    val partitioner = new RiakTSCoveragePlanBasedPartitioner(rc, "test", None, None, new Array[Filter](0), new ReadConf())
    val partitions = partitioner.partitions()
  }

  @Test
  @Category(Array(classOf[RegressionTests]))
  def checkPartitioningForIrregularData2(): Unit = {

    // host -> range(from->to)
    makeCoveragePlan(
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
        | {index: 0, queryData: {primaryHost: '1:0', entry: '[1,2)@1'}},
        | {index: 1, queryData: {primaryHost: '1:0', entry: '[3,4)@1'}},
        | {index: 2, queryData: {primaryHost: '2:0', entry: '[5,6)@2'}},
        | {index: 3, queryData: {primaryHost: '3:0', entry: '[9,10)@3'}},
        | {index: 4, queryData: {primaryHost: '2:0', entry: '[7,8)@2'}}
      ]""".stripMargin, partitions)
  }

  @Test
  def coveragePlanBasedPartitioningLessThanSplitCount(): Unit = {
    makeCoveragePlan(
      ("h1", 1 -> 2),
      ("h2", 3 -> 4),
      ("h3", 5 -> 6)
    )

    val rdd = new RiakTSRDD[Row](sc, rc, "test", None, None, None, filters)
    val partitions = rdd.partitions
      assertEqualsUsingJSONIgnoreOrder(
        """[
          | {index: 0, queryData: {primaryHost: 'h3:0', entry: '[5,6)@h3'}},
          | {index: 1, queryData: {primaryHost: 'h1:0', entry: '[1,2)@h1'}},
          | {index: 2, queryData: {primaryHost: 'h2:0', entry: '[3,4)@h2'}}
        ]""".stripMargin, partitions)
  }

  @Test
  def coveragePlanBasedPartitioningGreaterThanSplitCount(): Unit = {
    val requestedSplitCount = 3

    makeCoveragePlan(
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
    val rdd = new RiakTSRDD[Row](sc, rc, "test", None, None, None, filters, readConf = ReadConf(splitCount=requestedSplitCount))
    val partitions = rdd.partitions

    assertEqualsUsingJSONIgnoreOrder("""[
      | {index:0, queryData:[
      |     {primaryHost: 'h3:0', entry: '[14,15)@h3'},
      |     {primaryHost: 'h3:0', entry: '[16,17)@h3'},
      |     {primaryHost: 'h3:0', entry: '[18,19)@h3'}]},
      |
      | {index:1,queryData:[
      |     {primaryHost: 'h2:0', entry: '[6,7)@h2'},
      |     {primaryHost: 'h2:0', entry: '[8,9)@h2'},
      |     {primaryHost: 'h2:0', entry: '[10,11)@h2'},
      |     {primaryHost: 'h2:0', entry: '[12,13)@h2'}]},
      |
      | {index:2,queryData:[
      |     {primaryHost: 'h1:0', entry: '[1,2)@h1'},
      |     {primaryHost: 'h1:0', entry: '[3,4)@h1'},
      |     {primaryHost: 'h1:0', entry: '[5,6)@h1'}]}
      ]""".stripMargin, partitions)
  }

  private def makeCoveragePlan(entries: Tuple2[String, Tuple2[Int, Int]]*): Unit = {
    coveragePlan = new SimpleCoveragePlanResult

    entries.foreach(e => {
      val (host, range) = e

      val ce = new CoverageEntry()
      ce.setFieldName("time")
      ce.setHost(host)
      ce.setLowerBoundInclusive(true)
      ce.setLowerBound(range._1)
      ce.setUpperBoundInclusive(false)
      ce.setUpperBound(range._2)

      ce.setDescription(s"table / time >= ${range._1} AND time < ${range._2}")

      coveragePlan.addEntry(ce)
    })
  }

  private class RiakTSPartitionSerializer extends JsonSerializer[RiakTSPartition] {
    override def serialize(value: RiakTSPartition, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeStartObject()
      jgen.writeNumberField("index", value.index)

      jgen.writeFieldName("queryData")

      if (value.queryData.length >1) {
        jgen.writeStartArray()
      }

      value.queryData.foreach(qd => {
        jgen.writeStartObject()
        if (qd.primaryHost.isDefined) {
          jgen.writeObjectField("primaryHost", qd.primaryHost.get)
        }
        jgen.writeObjectField("entry", qd.coverageEntry)
        jgen.writeEndObject()
      })

      if (value.queryData.length >1) {
        jgen.writeEndArray()
      }

      jgen.writeEndObject()
    }
  }

  class RiakCoverageEntrySerializer extends JsonSerializer[CoverageEntry] {
    override def serialize(ce: CoverageEntry, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      val lb = ce.isLowerBoundInclusive match {
        case true => "["
        case false => "("
      }

      val ub = ce.isUpperBoundInclusive match {
        case true => "]"
        case false => ")"
      }

      jgen.writeString(s"$lb${ce.getLowerBound},${ce.getUpperBound}$ub@${ce.getHost}")
    }
  }
}