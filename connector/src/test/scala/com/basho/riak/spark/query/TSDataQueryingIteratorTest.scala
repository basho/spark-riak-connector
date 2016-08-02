/**
  * Copyright (c) 2015-2016 Basho Technologies, Inc.
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
package com.basho.riak.spark.query

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{Cell, ColumnDescription, Row}
import com.basho.riak.spark.rdd.connector.RiakConnector
import org.junit.Assert._
import org.junit.Test
import org.mockito.Matchers._
import org.mockito.Mockito._
import com.basho.riak.client.core.query.timeseries.{Row => RiakRow}
import com.basho.riak.spark.rdd.RegressionTests
import org.junit.experimental.categories.Category
import org.mockito.Matchers.{eq => mEq}

import scala.collection.JavaConversions._

class TSDataQueryingIteratorTest {

  @Test
  @Category(Array(classOf[RegressionTests]))
  def dataIteratorMustIterateThroughEmptyResponses(): Unit = {

    val nonEmptyTSD = TSQueryData("non-empty", None)

    val tsQD = List(
      TSQueryData("empty1", None),
      TSQueryData("empty2", None),
      TSQueryData("empty3", None),
      nonEmptyTSD,
      nonEmptyTSD)
    val emptyResponse = (Seq[ColumnDescription](), Seq[RiakRow]())
    val nonEmptyResponse = (Seq(new ColumnDescription("Col1", ColumnType.VARCHAR)), Seq(new RiakRow(List(new Cell("val1")))))

    val connector = mock(classOf[RiakConnector])
    val qTs = spy(QueryTS(connector, tsQD))

    // By default returns an emty results
    doReturn(emptyResponse).when(qTs).nextChunk(any[TSQueryData])

    // Return 1 row for non-empty results
    doReturn(nonEmptyResponse).when(qTs).nextChunk(mEq(nonEmptyTSD))

    val it = new TSDataQueryingIterator(qTs)

    assertEquals(2, it.size)
  }
}
