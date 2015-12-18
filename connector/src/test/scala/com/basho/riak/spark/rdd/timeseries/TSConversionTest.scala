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
package com.basho.riak.spark.rdd.timeseries

import java.sql.Timestamp
import java.util.{Calendar, Date}

import com.basho.riak.client.core.query.timeseries.{Cell, Row}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.util.TimeSeriesToSparkSqlConversion
import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._

class TSConversionTest extends Logging{

  case class SampleClass(int: Integer, date: Date, string: String)

  private def mkStructType(dataType: DataType*): StructType = {
    val fields = for {(dt, i) <-dataType.zipWithIndex} yield {
      StructField("f" +i, dt)
    }

    StructType(fields)
  }

  private def singleCellTest [T] (value: T, dataType: DataType, cell: Cell): Unit = {
    val singleCellRow = new Row(cell)
    val sparkRow = TimeSeriesToSparkSqlConversion.asSparkRow( mkStructType(dataType), singleCellRow)
    assertEquals(1, sparkRow.length)
    assertNotNull(sparkRow.getAs[T](0))
    assertSeqEquals(Seq(value), sparkRow.toSeq)
  }

  @Test
  def singleRowSingleIntCellTest(): Unit = {
    val int: Int = 1
    val singleCellRow = new Row(new Cell(int))
    val sparkRow = TimeSeriesToSparkSqlConversion.asSparkRow(mkStructType(IntegerType), singleCellRow)

    assertSeqEquals(Seq(int), sparkRow.toSeq)
  }

  @Test
  def singleRowSingleLongCellTest(): Unit = {
    val long = 1L // gets mapped as integer
    singleCellTest(long, LongType, new Cell(long))
  }

  @Test
  def singleRowSingleLongCellMaxValueTest(): Unit = {
    val long = Long.MaxValue
    singleCellTest(long, LongType, new Cell(long))
  }

  @Test
  def singleRowSingleStringCellTest(): Unit = {
    val string= "abc"
    singleCellTest(string, StringType, new Cell(string))
  }

  @Test
  def singleRowSingleBooleanCellTest(): Unit = {
    val bool = true
    singleCellTest(bool, BooleanType, new Cell(bool))
  }

  @Test
  def singleRowSingleDoubleCellTest(): Unit = {
    val double = 1.0d
    singleCellTest(double, DoubleType, new Cell(double))
  }

  @Test
  def singleRowSingleDoubleCellMaxValueTest(): Unit = {
    val double = Double.MaxValue
    singleCellTest(double, DoubleType, new Cell(double))
  }

  @Test
  def singleRowSingleBinaryValueCellTest(): Unit = {
    val string = "abc"
    val binary: BinaryValue = BinaryValue.create(string)
    val cell = new Cell(binary)
    singleCellTest(string, StringType, cell)
  }

  @Test
  def singleRowSingleDateCellTest(): Unit = {
    val millis = System.currentTimeMillis
    val date = new Date(millis)
    val cell = new Cell(date)
    singleCellTest(new Timestamp(millis), TimestampType, cell)
  }

  @Test
  def singleRowSingleCalendarCellTest(): Unit = {
    val millis = System.currentTimeMillis
    val cal = Calendar.getInstance
    cal.setTimeInMillis(millis)
    val cell = new Cell(cal)
    singleCellTest(new Timestamp(millis), TimestampType, cell)
  }

  @Test
  def singleRowMultipleCellsTest(): Unit = {
    val millis = System.currentTimeMillis
    val date = new Date(millis)

    val cells = List(new Cell(1), new Cell(date), new Cell("abc"))
    val row = new Row(cells)
    val sparkRow = TimeSeriesToSparkSqlConversion.asSparkRow(mkStructType(LongType, TimestampType, StringType),row)
    val expected = List(1L, new Timestamp(millis), "abc")
    assertSeqEquals(expected, sparkRow.toSeq)
  }

  private def compareElementwise(a: Seq[Any], b: Seq[Any]): Boolean = {
    a match {
      case Nil => true
      case x::xs =>
          val bh = b.head
          val a = x.equals(b.head)
          x.equals(b.head) && compareElementwise(xs, b.tail)
    }
  }

  private def assertSeqEquals(expected: Seq[Any], actual: Seq[Any]): Unit = {
    def stringify = (s: Seq[Any]) => s.mkString("[", ",", "]")
    if (expected.length != actual.length) {
      throw new AssertionError(s"Expected Seq of ${expected.length} elements, but got ${actual.length}")
    }

    if (!compareElementwise(expected, actual)) {
      throw new AssertionError(s"Expected Seq ${stringify(expected)} but got ${stringify(actual)}")
    }
  }
}

