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
package com.basho.riak.spark.util

import com.basho.riak.client.core.query.timeseries.{Cell, ColumnDescription, Row => RiakRow}
import com.basho.riak.spark.rdd.{TsTimestampBindingType, UseLong, UseTimestamp}
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.convert.decorateAll._
import java.sql.Timestamp

import org.apache.spark.sql.{Row => SparkRow}

import scala.reflect.ClassTag
import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import java.util.Calendar
import java.sql.Date

import scala.util.Try
import com.basho.riak.client.core.query.timeseries.FullColumnDescription
import com.basho.riak.client.core.query.timeseries.TableDefinition
import org.apache.spark.riak.types.RiakStructType

import scala.collection.JavaConversions._
import scala.util.Success
import scala.util.Failure

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
// TODO: consider using implicit decorations like: row.asSpark
object TSConversionUtil {
  val partitionKeyOrdinalProp = "riak.partitionKeyOrdinal"
  val localKeyOrdinalProp = "riak.localKeyOrdinal"
  val quantum = "riakTS.quantum"
  val quantizedField = "riakTS.quantizedField"
  
  private val STRING_TYPE_REFERENCE = new TypeReference[String] {}

  private def cellValue(sf: StructField, cell: Cell) = {
    if (cell != null) {
      sf.dataType match {
        case BooleanType =>
          cell.getBoolean

        case StringType =>
          cell.getVarcharAsUTF8String

        case LongType =>
          if (cell.hasTimestamp) cell.getTimestamp else cell.getLong

        case IntegerType =>
          cell.getLong.toInt

        case DoubleType =>
          cell.getDouble

        case TimestampType =>
          if (cell.hasLong) new Timestamp(cell.getLong) else new Timestamp(cell.getTimestamp)

        case _ =>
          throw new IllegalStateException(s"Unhandled cell type ${sf.dataType.typeName} for field ${sf.name}")
      }
    } else null
  }

  def asDataType(columnType: ColumnType, tsTimestampBinding: TsTimestampBindingType): DataType = {
    columnType match {
      case ColumnType.BOOLEAN => BooleanType
      case ColumnType.DOUBLE => DoubleType
      case ColumnType.SINT64 => LongType
      case ColumnType.TIMESTAMP => tsTimestampBinding match {
        case UseLong => LongType
        case UseTimestamp => TimestampType
      }
      case ColumnType.VARCHAR => StringType
      case _ => throw new IllegalStateException(s"Unsupported column type $columnType")
    }
  }

  def asColumnType(dataType: DataType) = {
    dataType match {
      case BooleanType => ColumnType.BOOLEAN
      case StringType => ColumnType.VARCHAR
      case LongType | IntegerType | ShortType | ByteType => ColumnType.SINT64
      case FloatType | DoubleType => ColumnType.DOUBLE
      case DateType | TimestampType => ColumnType.TIMESTAMP
      case _ => throw new IllegalStateException(s"$dataType is not supported")
    }
  }


  def asSparkRow(schema: StructType, row: RiakRow, columns: Option[Seq[ColumnDescription]] = None): SparkRow = {
    val cells = row.getCellsCopy.asScala
    val values = columns match {
      case None => (schema zip cells).map { case (n, v) => cellValue(n, v) }
      case Some(c) =>
        c zip cells map { case (cd, cell) => cellValue(schema(cd.getName), cell) }
    }
    new GenericRowWithSchema(values.toArray, schema)
  }

  private val classOfSparkRow = classOf[SparkRow]

  def from[T: ClassTag](columns: Seq[ColumnDescription], row: RiakRow)
                       (implicit schema: Option[StructType] = None, tsTimestampBinding: TsTimestampBindingType): T = {
    val ct = implicitly[ClassTag[T]]

    val (st, cs) = schema match {
      case Some(structType) => (new RiakStructType(structType.fields), Some(columns))
      case None             => (RiakStructType(columns, tsTimestampBinding), None)
    }
    asSparkRow(st, row, cs).asInstanceOf[T]
  }

  private def getCell[T](row: SparkRow, name: String): Cell = {
    val value = Try(row.getAs[T](name))
    value match {
      case Success(result) => asCell(result)
      case Failure(_)      => null
    }
  }
  
  def createRiakRowBySchema[T <: SparkRow](row: T, schema: StructType): (RiakRow, Option[Seq[ColumnDescription]]) = {
    val cellsWithDescription = schema.fields.map(field => getCellAndDescription(row, field))
    val (descriptions, cells) = cellsWithDescription.unzip
    new RiakRow(cells: _*) -> Option(descriptions)
  }

  private def getCellAndDescription[T <: SparkRow](row: T, field: StructField): (ColumnDescription, Cell) = field.dataType match {
    case BooleanType =>
      val cd = new ColumnDescription(field.name, ColumnType.BOOLEAN)
      val cell = getCell[Boolean](row, field.name)
      cd -> cell
    case StringType =>
      val cd = new ColumnDescription(field.name, ColumnType.VARCHAR)
      val cell = getCell[String](row, field.name)
      cd -> cell
    case LongType =>
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = getCell[Long](row, field.name)
      cd -> cell
    case IntegerType =>
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = getCell[Int](row, field.name)
      cd -> cell
    case ShortType =>
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = getCell[Short](row, field.name)
      cd -> cell
    case ByteType =>
      val cd = new ColumnDescription(field.name, ColumnType.SINT64)
      val cell = getCell[Byte](row, field.name)
      cd -> cell
    case FloatType =>
      val cd = new ColumnDescription(field.name, ColumnType.DOUBLE)
      val cell = getCell[Float](row, field.name)
      cd -> cell
    case DoubleType =>
      val cd = new ColumnDescription(field.name, ColumnType.DOUBLE)
      val cell = getCell[Double](row, field.name)
      cd -> cell
    case DateType =>
      val cd = new ColumnDescription(field.name, ColumnType.TIMESTAMP)
      val cell = getCell[java.sql.Date](row, field.name)
      cd -> cell
    case TimestampType =>
      val cd = new ColumnDescription(field.name, ColumnType.TIMESTAMP)
      val cell = getCell[java.sql.Timestamp](row, field.name)
      cd -> cell
    case _ => sys.error(s"${field.dataType} not supported")
  }

  private def asCell[T](value: T): Cell = {
    value match {
      case null                  => null
      case v: Boolean            => new Cell(v)
      case v: String             => new Cell(v)
      case v: Float              => new Cell(v.toDouble)
      case v: Double             => new Cell(v)
      case v: Long               => new Cell(v)
      case v: Int                => new Cell(v.toLong)
      case v: Short              => new Cell(v.toLong)
      case v: Byte               => new Cell(v.toLong)
      case v: Calendar           => new Cell(v)
      case v: java.sql.Date      => Cell.newTimestamp(v.getTime)
      case v: java.sql.Timestamp => Cell.newTimestamp(v.getTime)
      case v: java.util.Date     => Cell.newTimestamp(v.getTime)
      case _                     => sys.error(s"Value of class ${value.getClass} not supported")
    }
  }

  def createRowByType[T <: SparkRow](row: T): RiakRow = {
    val cells = for {i <- 0 until row.size} yield {
      asCell(row(i))
    }
    new RiakRow(cells: _*)
  }
}
