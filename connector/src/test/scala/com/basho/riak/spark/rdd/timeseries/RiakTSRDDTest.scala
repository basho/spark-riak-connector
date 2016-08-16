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
package com.basho.riak.spark.rdd.timeseries

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{Cell, ColumnDescription}
import com.basho.riak.spark.query.{QueryTS, TSQueryData}
import com.basho.riak.spark.rdd.{RegressionTests, RiakTSRDD}
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.partitioner.RiakTSPartition
import org.apache.spark.{SparkContext, TaskContext}
import org.junit.Assert._
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.powermock.modules.junit4.PowerMockRunner
import org.powermock.api.mockito.PowerMockito
import com.basho.riak.client.core.query.timeseries.{Row => RiakRow}
import org.apache.spark.sql.{Row => SparkRow}
import org.junit.experimental.categories.Category
import org.mockito.Mock
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.powermock.core.classloader.annotations.PrepareForTest

import scala.collection.JavaConversions._

@RunWith(classOf[PowerMockRunner])
@PrepareForTest(Array(classOf[QueryTS],classOf[RiakTSRDD[_]]))
class RiakTSRDDTest {

  @Mock
  protected val rc: RiakConnector = null

  @Mock
  protected val sc: SparkContext = null

  @Mock
  protected val tc: TaskContext = null

  @Test
  @Category(Array(classOf[RegressionTests]))
  def readAll(): Unit = {
    val rdd = new RiakTSRDD[SparkRow](sc, rc, "test")

    val neTsQD = TSQueryData("non-empty", None)
    val nonEmptyResponse = (Seq(new ColumnDescription("Col1", ColumnType.VARCHAR), new ColumnDescription("Col2", ColumnType.SINT64)),
      Seq(new RiakRow(List(new Cell("string-value"), new Cell(42)))))

    PowerMockito.whenNew(classOf[QueryTS]).withAnyArguments().thenAnswer( new Answer[QueryTS]{
      override def answer(invocation: InvocationOnMock): QueryTS = {
        val args = invocation.getArguments
        val rc: RiakConnector = args(0).asInstanceOf[RiakConnector]
        val qd: Seq[TSQueryData] = args(1).asInstanceOf[Seq[TSQueryData]]

        val q = spy(QueryTS(rc, qd))

        // By default returns an empty result
        doReturn(Seq() -> Seq()).when(q).nextChunk(any[TSQueryData])

        // Return 1 row for non-empty result
        doReturn(nonEmptyResponse).when(q).nextChunk(org.mockito.Matchers.eq(neTsQD))

        q
      }
    })

    // ----------  Perform test
    val iterator = rdd.compute( RiakTSPartition(0, Nil, List( null, null, null, neTsQD, neTsQD, null, neTsQD, null)), tc)

    // ----------  verify results
    val seq: Seq[SparkRow] = iterator.toIndexedSeq

    /**
      * NOTE: Asserts willbe used, since we can't use assertEqualsUsingJSONIgnoreOrder() here because of the following issue
      *       that possible introduced by a combination of jackson-scala-module and PowerMock:
      *
      *  Caused by: java.lang.NullPointerException
      *   [error]     at com.fasterxml.jackson.module.scala.JacksonModule$.<init>(JacksonModule.scala:18)
      *   [error]     at com.fasterxml.jackson.module.scala.JacksonModule$.<clinit>(JacksonModule.scala)
      *
      */
    assertEquals(3, seq.size)

    seq.foreach(r => {
      assertEquals(2, r.size)
      assertEquals("string-value", r.get(0))
      assertEquals(42l, r.get(1))
    })
  }
}
