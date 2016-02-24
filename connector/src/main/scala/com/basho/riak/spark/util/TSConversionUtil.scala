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
import com.basho.riak.spark.rdd.{UseTimestamp, UseLong, TsTimestampBindingType}
import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import scala.collection.convert.decorateAll._
import java.sql.Timestamp
import org.apache.spark.sql.{Row => SparkRow}

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

  def asDataType(columnType: String, tsTimestampBinding:TsTimestampBindingType): DataType = {
    asDataType(ColumnType.valueOf(columnType.toUpperCase()), tsTimestampBinding)
  }

  def asDataType(columnType: ColumnType, tsTimestampBinding: TsTimestampBindingType): DataType =
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

  private def asStructField(columnDescription: ColumnDescription, tsTimestampBinding:TsTimestampBindingType): StructField = {
    val ft = asDataType(columnDescription.getType, tsTimestampBinding)
    StructField(columnDescription.getName, ft)
  }

  def asSparkRow(schema: StructType, row: Row, columns: Option[Seq[ColumnDescription]] = None): SparkRow = {
    val values = columns match {
      case None => (schema zip row.getCellsCopy.asScala).map { case (n, v) => cellValue(n, v) }
      case Some(c) =>
        c.zipWithIndex.map { case (cd, i) =>
          cellValue(schema(cd.getName), row.getCellsCopy.asScala(i))
        }
    }
    new GenericRowWithSchema(values.toArray, schema)
  }

  def asSparkSchema(columns: Seq[ColumnDescription], tsTimestampBinding:TsTimestampBindingType): StructType =
    StructType(columns.map(c => asStructField(c, tsTimestampBinding)))

  private def validateSchema(schema: StructType, columns: Seq[String]): Unit = {
    val msg = "Provided schema does not match the riak row columns"
    require(schema.length == columns.length, msg)
    require(schema.forall(sf => columns.contains(sf.name)), msg)
  }
}

object TSConversionUtil {
  private val classOfSparkRow = classOf[SparkRow]

  def from[T: ClassTag](columns: Seq[ColumnDescription], row: Row)
                       (implicit schema: Option[StructType] = None, tsTimestampBinding:TsTimestampBindingType): T = {
    val ct = implicitly[ClassTag[T]]

    if (ct.runtimeClass == classOfSparkRow) {
      (schema match {
        case Some(structType) => (structType, Some(columns))
        case None => (TimeSeriesToSparkSqlConversion.asSparkSchema(columns, tsTimestampBinding), None)
      }) match {
        case (st, cs) => TimeSeriesToSparkSqlConversion.asSparkRow(st, row, cs).asInstanceOf[T]
      }
    } else {
      row.asInstanceOf[T]
    }
  }
}
