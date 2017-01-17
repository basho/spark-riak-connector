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

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType._
import com.basho.riak.client.core.query.timeseries.{Cell, ColumnDescription, Row}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.util.TSConversionUtil
import org.apache.spark.riak.Logging
import org.apache.spark.sql.types._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConversions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

class TSConversionTest extends Logging {

  case class SampleClass(int: Integer, date: Date, string: String)

  private def mkStructType(dataType: DataType*): StructType =
    StructType(dataType.zipWithIndex.map { case (dt, i) => StructField("f" + i, dt) })

  private def mkColumnDescriptions(dataType: ColumnDescription.ColumnType*): Seq[ColumnDescription] =
    dataType.zipWithIndex.map { case (dt, i) => new ColumnDescription("f" + i, dt) }

  private def singleCellTest[T](value: T, dataType: DataType, cell: Cell, columns: Option[Seq[ColumnDescription]] = None): Unit = {
    val singleCellRow = new Row(cell)
    val sparkRow = TSConversionUtil.asSparkRow(mkStructType(dataType), singleCellRow, columns)
    assertEquals(1, sparkRow.length)
    assertNotNull(sparkRow.getAs[T](0))
    assertEquals(dataType, sparkRow.schema(0).dataType)
    columns match {
      case Some(cs) if value.isInstanceOf[Timestamp] && dataType == LongType =>
        assertSeqEquals(Seq(value.asInstanceOf[Timestamp].getTime), sparkRow.toSeq) // check timestamp-to-long conversion
      case Some(cs) if value.isInstanceOf[Long] && dataType == TimestampType =>
        assertSeqEquals(Seq(new Timestamp(value.asInstanceOf[Long])), sparkRow.toSeq) // check long-to-timestamp conversion
      case None => assertSeqEquals(Seq(value), sparkRow.toSeq)
    }
  }

  @Test
  def singleRowSingleIntCellTest(): Unit = {
    val int: Int = 1
    val singleCellRow = new Row(new Cell(int))
    val sparkRow = TSConversionUtil.asSparkRow(mkStructType(IntegerType), singleCellRow)

    assertSeqEquals(Seq(int), sparkRow.toSeq)
  }

  @Test
  def singleRowSingleLongCellTest(): Unit = {
    val long = 1L // gets mapped as integer
    singleCellTest(long, LongType, new Cell(long))
  }

  @Test
  def singleRowSingleTimestampToLongCellTest(): Unit = {
    val timestamp = new Timestamp(1L) // gets mapped as integer
    singleCellTest(timestamp, LongType, new Cell(timestamp), Some(mkColumnDescriptions(TIMESTAMP)))
  }

  @Test
  def singleRowSingleLongToTimestampCellTest(): Unit = {
    val long = 1L // gets mapped as integer
    singleCellTest(long, TimestampType, new Cell(long), Some(mkColumnDescriptions(SINT64)))
  }

  @Test
  def singleRowSingleLongCellMaxValueTest(): Unit = {
    val long = Long.MaxValue
    singleCellTest(long, LongType, new Cell(long))
  }

  @Test
  def singleRowSingleStringCellTest(): Unit = {
    val string = "abc"
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
    val sparkRow = TSConversionUtil.asSparkRow(mkStructType(LongType, TimestampType, StringType), row)
    val expected = List(1L, new Timestamp(millis), "abc")
    assertSeqEquals(expected, sparkRow.toSeq)
  }

  @Test
  def singleRowMultipleCellsTimestampToLongTest(): Unit = {
    val millis = System.currentTimeMillis
    val date = new Date(millis)

    val cells = List(new Cell(1), new Cell(date), new Cell("abc"))
    val row = new Row(cells)
    val columnDescriptions = Some(mkColumnDescriptions(SINT64, TIMESTAMP, VARCHAR))
    val sparkRow = TSConversionUtil.asSparkRow(
      mkStructType(LongType, LongType, StringType), row, columnDescriptions)
    val expected = List(1L, millis, "abc")
    assertSeqEquals(expected, sparkRow.toSeq)
  }

  @Test
  def singleRowMultipleCellsLongToTimestampTest(): Unit = {
    val millis = System.currentTimeMillis
    val date = new Date(millis)

    val cells = List(new Cell(1), new Cell(date), new Cell("abc"))
    val row = new Row(cells)
    val columnDescriptions = Some(mkColumnDescriptions(SINT64, SINT64, VARCHAR))
    val sparkRow = TSConversionUtil.asSparkRow(
      mkStructType(LongType, TimestampType, StringType), row, columnDescriptions)
    val expected = List(1L, new Timestamp(millis), "abc")
    assertSeqEquals(expected, sparkRow.toSeq)
  }
  
  @Test
  def toRiakRowTest(): Unit = {
    val schema = mkStructType(LongType, TimestampType, StringType)
    val millis = System.currentTimeMillis
    val date = new Date(millis)
    val values = Array(1, date, "abc")
    val sparkRow = new GenericRowWithSchema(values, schema)
    val riakRow = TSConversionUtil.createRowByType(sparkRow)
    
    val cells = List(new Cell(1), new Cell(date), new Cell("abc"))
    assertSeqEquals(cells, riakRow.getCellsCopy)
  }
  
  @Test
  def toRiakRowAndBackTest(): Unit = {
    val schema = mkStructType(LongType, TimestampType, StringType)
    val millis = System.currentTimeMillis
    val date = new Timestamp(millis)
    val values = Array(1L, date, "abc")
    val sparkRow = new GenericRowWithSchema(values, schema)
    val riakRow = TSConversionUtil.createRowByType(sparkRow)
    val columnDescriptions = Some(mkColumnDescriptions(SINT64, TIMESTAMP, VARCHAR))
    val sparkRow1 = TSConversionUtil.asSparkRow(schema, riakRow, columnDescriptions)
    
    assertSeqEquals(sparkRow.toSeq.toList, sparkRow1.toSeq.toList)
  }
  
  @Test
  def nullableConversionTest(): Unit = {
    val millis = System.currentTimeMillis
    val date = new Date(millis)

    val cells = List(new Cell(1), new Cell(date), null)
    val row = new Row(cells)
    val columnDescriptions = Some(mkColumnDescriptions(SINT64, SINT64, VARCHAR))
    val sparkRow = TSConversionUtil.asSparkRow(
      mkStructType(LongType, TimestampType, StringType), row, columnDescriptions)
    val expected = List(1L, new Timestamp(millis), null)
    assertSeqEquals(expected, sparkRow.toSeq)
  }

  private def compareElementwise(a: Seq[Any], b: Seq[Any]): Boolean = a match {
    case Nil => true
    case null :: xs => b.head == null && compareElementwise(xs, b.tail)
    case x :: xs => x.equals(b.head) && compareElementwise(xs, b.tail)
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

