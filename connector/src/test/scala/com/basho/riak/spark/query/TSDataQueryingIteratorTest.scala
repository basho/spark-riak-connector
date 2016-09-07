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

import com.basho.riak.JsonTestFunctions
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
import org.junit.runner.RunWith
import org.mockito.Matchers.{eq => mEq}
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[MockitoJUnitRunner])
class TSDataQueryingIteratorTest extends JsonTestFunctions{

  @Mock
  private val rc: RiakConnector = null

  private val nonEmptyTsQD1 = TSQueryData("non-empty1", None)
  private val nonEmptyTsQD2 = TSQueryData("non-empty2", None)
  private val nonEmptyTsQD3 = TSQueryData("non-empty3", None)

  val emptyResponse = (Seq(), Seq())

  val nonEmptyResponse1 = (
    Seq(
      new ColumnDescription("Col1", ColumnType.VARCHAR)),
    Seq(
      new RiakRow(List(new Cell("response1")))))

  val nonEmptyResponse2 = (
    Seq(
      new ColumnDescription("Col1", ColumnType.VARCHAR),
      new ColumnDescription("Col2", ColumnType.SINT64)),
    Seq(
      new RiakRow(List(
        new Cell("response2"),
        new Cell(42)))))

  val nonEmptyResponse3 = (
    Seq(
      new ColumnDescription("Col1", ColumnType.VARCHAR),
      new ColumnDescription("Col2", ColumnType.SINT64),
      new ColumnDescription("Col3", ColumnType.TIMESTAMP)),
    Seq(new RiakRow(List(
      new Cell("response3"),
      new Cell(42),
      Cell.newTimestamp(System.currentTimeMillis()))))
  )

  /**
    * There was a bug: iterator didn't iterate over the next subquery in case when previous subquery returned no data as a result
    * incomplete data were returned.
    */
  @Test
  @Category(Array(classOf[RegressionTests]))
  def dataIteratorMustIterateThroughEmptyResponses(): Unit = {
    val q = mkQueryTSpy(null, null, null, nonEmptyTsQD1, nonEmptyTsQD2)
    val it = new TSDataQueryingIterator(q)
    verifyIterator(Seq(nonEmptyResponse1, nonEmptyResponse2), it)
  }

  /**
    * There was a logical disconnect between value returned by [[TSDataQueryingIterator.columnDefs]] and the actual data
    * that was returned by the iterator:
    *
    *   The [[TSDataQueryingIterator.columnDefs]] property was initialized by the subquery that returns non null definitions
    * and as a result in case when the firs subquery returns no data and column definition was empty -
    * it always returns an empty column defs even when subquery returns a real results
    * that include corresponding column definitions.
    */
  @Test
  @Category(Array(classOf[RegressionTests]))
  def ensureThatColumnDefinitionsReflectsToTheOriginalRespone(): Unit = {
    val q = mkQueryTSpy(null, null, null, nonEmptyTsQD1, nonEmptyTsQD2,
      null, null, nonEmptyTsQD3, null)

    val it = new TSDataQueryingIterator(q)
    verifyIterator(Seq(nonEmptyResponse1, nonEmptyResponse2, nonEmptyResponse3), it)
  }

  private def mkQueryTSpy(queryData: TSQueryData*): QueryTS = {
    val q = spy(QueryTS(rc, queryData))

    // By default returns an empty results
    doReturn(emptyResponse).when(q).nextChunk(any[TSQueryData])

    doReturn(nonEmptyResponse1).when(q).nextChunk(mEq(nonEmptyTsQD1))
    doReturn(nonEmptyResponse2).when(q).nextChunk(mEq(nonEmptyTsQD2))
    doReturn(nonEmptyResponse3).when(q).nextChunk(mEq(nonEmptyTsQD3))
    q
  }

  @Test
  def shouldReturnNilColumnDefs_if_noDataAtAll(): Unit = {
    val q = mkQueryTSpy(null, null, null, null)
    val it = new TSDataQueryingIterator(q)

    val cd = it.columnDefs
    assertFalse(it.hasNext)
    assertEquals(Nil, cd)
  }

  private def verifyIterator(expected: Seq[(Seq[ColumnDescription], Seq[RiakRow])], iterator: TSDataQueryingIterator): Unit = {
    val expectedIterator = expected.iterator

    var theLastExpectedItem:Option[(Seq[ColumnDescription], Seq[RiakRow])] = None

    iterator.foreach( r => {
      assertTrue("Expected less data then it actually is", expectedIterator.hasNext)
      theLastExpectedItem = Some(expectedIterator.next())
      val columns = iterator.columnDefs

      assertEqualsUsingJSONIgnoreOrder(theLastExpectedItem.get, columns -> Seq(r))
    })

    assertFalse("Expected more data then it actually is", expectedIterator.hasNext)

    val cd1 = iterator.columnDefs
    assertFalse(iterator.hasNext)
    assertEqualsUsingJSONIgnoreOrder(theLastExpectedItem.getOrElse(Seq()->Seq())._1, cd1)
  }
}