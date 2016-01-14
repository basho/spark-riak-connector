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

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{Cell, Row, ColumnDescription}
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import scala.collection.convert.decorateAll._
import java.sql.Timestamp

import scala.reflect.ClassTag

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
// TODO: consider using implicit decorations like: row.asSpark
object TimeSeriesToSparkSqlConversion {
  private val STRING_TYPE_REFERENCE = new TypeReference[String] {}

  private def cellValue(sf: StructField, cell: Cell) = sf.dataType match {
    case BooleanType =>
      cell.getBoolean

    case StringType =>
      cell.getVarcharAsUTF8String

    case LongType =>
      cell.getLong

    case IntegerType =>
      cell.getLong.toInt

    case DoubleType =>
      cell.getDouble

    case TimestampType =>
      new Timestamp(cell.getTimestamp)

    case _ =>
      throw new IllegalStateException(s"Unhandled cell type ${sf.dataType.typeName}")
  }

  def asDataType(columnType: String): DataType = {
    asDataType( ColumnType.valueOf(columnType.toUpperCase()))
  }

  def asDataType(columnType: ColumnType): DataType =
    columnType match {
      case ColumnType.BOOLEAN => BooleanType
      case ColumnType.DOUBLE => DoubleType
      case ColumnType.SINT64 => LongType
      case ColumnType.TIMESTAMP => TimestampType
      case ColumnType.VARCHAR => StringType
      case _ => throw new IllegalStateException("Unsupported column type '" + columnType + "'")
    }

  private def asStructField(columnDescription: ColumnDescription): org.apache.spark.sql.types.StructField = {
    val ft = asDataType(columnDescription.getType )
    StructField(columnDescription.getName, ft)
  }

  def asSparkRow(schema: StructType, row: Row): org.apache.spark.sql.Row = {
    val values = for {c <- schema zip row.getCellsCopy.asScala} yield {
      cellValue(c._1, c._2)
    }
    new GenericRowWithSchema(values.toArray, schema)
  }

  def asSparkSchema(columns: Seq[ColumnDescription]): StructType = {
    val fields = for {c <- columns} yield {
      asStructField(c)
    }
    StructType(fields)
  }
}

object TSConversionUtil {
  private val classOfSparkRow = classOf[org.apache.spark.sql.Row]


  def from[T: ClassTag](columns: Seq[ColumnDescription], row: Row): T = {
    val ct = implicitly[ClassTag[T]]

    if (ct.runtimeClass == classOfSparkRow) {
      val schema = TimeSeriesToSparkSqlConversion.asSparkSchema(columns)
      TimeSeriesToSparkSqlConversion.asSparkRow(schema, row).asInstanceOf[T]
    } else {
      row.asInstanceOf[T]
    }
  }
}
