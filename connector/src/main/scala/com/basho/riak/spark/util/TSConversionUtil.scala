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
package com.basho.riak.spark.util

import com.basho.riak.client.core.query.timeseries.ColumnDescription.ColumnType
import com.basho.riak.client.core.query.timeseries.{Cell, Row, ColumnDescription}
import com.fasterxml.jackson.core.`type`.TypeReference
import scala.collection.convert.decorateAll._

import scala.reflect.ClassTag

// TODO: consider using implicit decorations like: row.asSpark
object TimeSeriesToSparkSqlConversion {
  private val STRING_TYPE_REFERENCE = new TypeReference[String] {}

  // Will be removed when corresponding method be introduced in the Cell
  private def cellType(cell: Cell): ColumnType = {
    if (cell.hasVarcharValue) {
      ColumnType.VARCHAR
    } else if (cell.hasBoolean) {
      ColumnType.BOOLEAN
    } else if (cell.hasLong) {
      ColumnType.SINT64
    } else if (cell.hasDouble) {
      ColumnType.DOUBLE
    } else if (cell.hasTimestamp) {
      ColumnType.TIMESTAMP
    } else {
      throw new IllegalArgumentException("Unknown cell type")
    }
  }

  private def cellValue(cell: Cell) = cellType(cell) match {
    case ColumnType.VARCHAR =>
      cell.getVarcharAsUTF8String

    case ColumnType.BOOLEAN =>
      cell.getBoolean

    case ColumnType.SINT64 =>
      cell.getLong

    case ColumnType.DOUBLE =>
        cell.getDouble

    case ColumnType.TIMESTAMP =>
      cell.getTimestamp

    case _ =>
      throw new IllegalStateException(s"Unhandled cell type ${cellType(cell).name()}")
  }

  def asSparkRow(row: Row): org.apache.spark.sql.Row = {
    org.apache.spark.sql.Row.apply(
      row.getCells.asScala map (cell => cellValue(cell)):_*
    )
  }
}

object TSConversionUtil {
  private val classOfSparkRow = classOf[org.apache.spark.sql.Row]


  def from[T: ClassTag](columns: Seq[ColumnDescription], row: Row): T = {
    val ct = implicitly[ClassTag[T]]

    if (ct.runtimeClass == classOfSparkRow) {
      TimeSeriesToSparkSqlConversion.asSparkRow(row).asInstanceOf[T]
    } else {
      row.asInstanceOf[T]
    }
  }
}
