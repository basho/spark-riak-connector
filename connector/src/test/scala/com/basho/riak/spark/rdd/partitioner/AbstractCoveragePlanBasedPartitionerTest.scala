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

import com.basho.riak.JsonTestFunctions
import com.basho.riak.client.api.commands.kv.{CoveragePlan => KVCoveragePlan}
import com.basho.riak.client.core.query.timeseries
import com.basho.riak.client.api.commands.timeseries.{CoveragePlan => TSCoveragePlan}
import com.basho.riak.client.core.operations.CoveragePlanOperation
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.spark.rdd.connector.{RiakConnector, RiakSession}
import com.fasterxml.jackson.core.{JsonGenerator, Version}
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.SparkContext
import org.junit.Before
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Matchers._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.Mockito._

import scala.collection.Map

/**
  * Created by srg on 2/28/17.
  */
class AbstractCoveragePlanBasedPartitionerTest extends JsonTestFunctions {

  @Mock
  protected val rc: RiakConnector = null

  @Mock
  protected val rs: RiakSession = null

  @Mock
  protected val sc: SparkContext = null

  // To access protected constructor in CoveragePlanResult
  protected class SimpleTSCoveragePlanResult extends timeseries.CoveragePlanResult

  protected class SimpleKVCoveragePlanOperationResponse extends CoveragePlanOperation.Response

  protected var tsCoveragePlan: SimpleTSCoveragePlanResult = null
  protected var kvCoveragePlan: KVCoveragePlan.Response = null

  override protected def tolerantMapper: ObjectMapper = super.tolerantMapper
    .registerModule(
      new SimpleModule("RiakTs2 Module", new Version(1, 0, 0, null))
        .addSerializer(classOf[RiakTSPartition], new RiakTSPartitionSerializer)
        .addSerializer(classOf[timeseries.CoverageEntry], new RiakTSCoverageEntrySerializer)
        .addSerializer(classOf[CoveragePlanOperation.Response.CoverageEntry], new RiakKVCoverageEntrySerializer())
        .addSerializer(classOf[RiakLocalCoveragePartition[Any]], new RiakKVPartitionSerializer())
    )

  @Before
  def initializeMocks(): Unit = {
    MockitoAnnotations.initMocks(this);
    doAnswer(new Answer[timeseries.CoveragePlanResult] {
      override def answer(invocation: InvocationOnMock): timeseries.CoveragePlanResult = {
        tsCoveragePlan
      }
    }).when(rs).execute(isA(classOf[TSCoveragePlan]))

    doAnswer(new Answer[KVCoveragePlan.Response] {
      override def answer(invocation: InvocationOnMock): KVCoveragePlan.Response = {
        kvCoveragePlan
      }
    }).when(rs).execute(isA(classOf[KVCoveragePlan]))

    doAnswer(new Answer[AnyRef] {
      override def answer(invocation: InvocationOnMock) = {
        val func = invocation.getArguments()(0).asInstanceOf[RiakSession => AnyRef]
        func.apply(rs)
      }
    }).when(rc).withSessionDo(any(classOf[Function1[RiakSession, timeseries.CoveragePlanResult]]))

    mockSparkExecutorsNumber(2)
    tsCoveragePlan = new SimpleTSCoveragePlanResult
  }

  protected def mockKVCoveragePlan(entries: Tuple2[String, Any]*): Unit = {
    val r = new SimpleKVCoveragePlanOperationResponse()
    val m = classOf[CoveragePlanOperation.Response].getDeclaredMethod("addEntry", classOf[CoveragePlanOperation.Response.CoverageEntry])
    m.setAccessible(true)

    entries.foreach(e => {
      val (host, d) = e

      val ce = new CoveragePlanOperation.Response.CoverageEntry()
      FieldUtils.writeField(ce, "host", host, true)
      FieldUtils.writeField(ce, "port", 0, true)
      FieldUtils.writeField(ce, "description", d.toString, true)
      FieldUtils.writeField(ce, "coverageContext", ce.getDescription.getBytes, true)

      m.invoke(r, ce)
    })

    val constructor = classOf[KVCoveragePlan.Response].getDeclaredConstructor(classOf[CoveragePlanOperation.Response])
    constructor.setAccessible(true)
    kvCoveragePlan = constructor.newInstance(r)
  }

  protected def mockSparkExecutorsNumber(num: Int): Unit = {
    val d = for {
      i <- num to 1 by -1
      t = s"ex$i"->(12L,12L)
    } yield t

    when(sc.getExecutorMemoryStatus).thenReturn( Map[String, (Long, Long)](d: _*))
  }

  protected def mockTSCoveragePlan(entries: Tuple2[String, Tuple2[Int, Int]]*): Unit = {
    tsCoveragePlan = new SimpleTSCoveragePlanResult

    entries.foreach(e => {
      val (host, range) = e

      val ce = new timeseries.CoverageEntry()
      ce.setFieldName("time")
      ce.setHost(host)
      ce.setLowerBoundInclusive(true)
      ce.setLowerBound(range._1)
      ce.setUpperBoundInclusive(false)
      ce.setUpperBound(range._2)

      ce.setDescription(s"table / time >= ${range._1} AND time < ${range._2}")

      tsCoveragePlan.addEntry(ce)
    })
  }

  private class RiakKVPartitionSerializer extends JsonSerializer[RiakLocalCoveragePartition[Any]] {
    override def serialize(value: RiakLocalCoveragePartition[Any], jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeStartObject()
        jgen.writeNumberField("index", value.index)
        jgen.writeObjectField("primaryHost", value.primaryHost)

        jgen.writeObjectFieldStart("queryData")
          jgen.writeObjectField("entries", value.queryData.coverageEntries.getOrElse(Seq.empty))
        jgen.writeEndObject()
      jgen.writeEndObject()
    }
  }

  private class RiakKVCoverageEntrySerializer extends JsonSerializer[CoveragePlanOperation.Response.CoverageEntry] {
    override def serialize(value: CoverageEntry, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      jgen.writeStartObject()
        jgen.writeStringField("host", value.getHost + ":" + value.getPort)
        jgen.writeStringField("description", value.getDescription)
      jgen.writeEndObject()
    }
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

  class RiakTSCoverageEntrySerializer extends JsonSerializer[timeseries.CoverageEntry] {
    override def serialize(ce: timeseries.CoverageEntry, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
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
