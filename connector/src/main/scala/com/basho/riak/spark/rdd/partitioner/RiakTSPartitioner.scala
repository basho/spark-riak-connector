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
package com.basho.riak.spark.rdd.partitioner

import java.sql.Timestamp
import org.apache.spark.Partition
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{ StructType, TimestampType }
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.query.TSQueryData
import com.basho.riak.spark.rdd.{ ReadConf, RiakPartition }

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 */
case class RiakTSPartition(
  index: Int,
  endpoints: Iterable[HostAndPort],
  queryData: TSQueryData) extends RiakPartition

trait RiakTSPartitioner {

  protected def toSql(columnNames: Option[Seq[String]], tableName: String, schema: Option[StructType], whereConstraints: (String, Seq[Any])): (String, Seq[Any]) = {
    var values: Seq[Any] = Seq.empty[Nothing]
    val sql = "SELECT " +
      (
        columnNames match {
          case None => schema match {
            case Some(s) => s.fieldNames.mkString(", ")
            case None    => "*"
          }
          case Some(c: Seq[String]) => c.mkString(", ")
        }) +
        s" FROM $tableName " +
        (
          whereConstraints match {
            case (n, vs) => {
              values = vs.map {
                case (_, v) => v
                case v      => v
              }
              s" WHERE $n"
            }
          })
    (sql, values)
  }
  
  
  /** Construct Sql clause */
  protected def filterToSqlAndValue(filter: Any): (String, Any) = {
    val (attribute, sqlOperator, value) = filter match {
      case EqualTo(a, v)            => (a, "=", v)
      case LessThan(a, v)           => (a, "<", v)
      case LessThanOrEqual(a, v)    => (a, "<=", v)
      case GreaterThan(a, v)        => (a, ">", v)
      case GreaterThanOrEqual(a, v) => (a, ">=", v)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <= and =  are allowed.")
    }

    // TODO: need to add pattern matching for values, to be sure that they are used correctly
    (s"$attribute $sqlOperator ?", value)
  }

  protected def whereClause(filters: Array[Filter]): (String, Seq[Any]) = {
    val sqlValue = filters.map(filterToSqlAndValue)
    val sql = sqlValue.map(_._1).mkString(" AND ")
    val args = sqlValue.map(_._2)
    (sql, args.seq)
  }
}

/** Returns single partition with initial query */
object SinglePartitionRiakTSPartitioner extends RiakTSPartitioner {
  def partitions(endpoints: Iterable[HostAndPort], tableName: String, schema: Option[StructType],
                    columnNames: Option[Seq[String]], query: Option[String], whereConstraints: Option[(String, Seq[Any])], filters: Array[Filter]): Array[Partition] = {
    val (sql, values) = query match {
      case Some(q) => (q, Seq.empty[Nothing])
      case None    => {
       val where = whereConstraints match {
          case Some(w) => w
          case None    => whereClause(filters)
       }
       toSql(columnNames, tableName, schema, where) 
      }
    }
    Array(new RiakTSPartition(0, endpoints, TSQueryData(sql, values)))
  }
}

/** Splits initial range query into readConf.splitCount number of sub-ranges, each in a separate partition */
object RangedRiakTSPartitioner extends RiakTSPartitioner {

  private def getFilterAttribute(filter: Filter) = filter match {
    case EqualTo(a, v)            => a
    case GreaterThan(a, v)        => a
    case LessThan(a, v)           => a
    case GreaterThanOrEqual(a, v) => a
    case LessThanOrEqual(a, v)    => a
    case _                        => null
  }

  private def getMin(filters: Array[Filter]): Option[Long] = {
    filters.flatMap {
      case GreaterThanOrEqual(attr, v: Long)      => Some(v)
      case GreaterThan(attr, v: Long)             => Some(v + 1)
      case GreaterThanOrEqual(attr, v: Timestamp) => Some(v.getTime)
      case GreaterThan(attr, v: Timestamp)        => Some(v.getTime + 1)
      case _                                      => None
    }.headOption
  }

  private def getMax(filters: Array[Filter]): Option[Long] = {
    filters.flatMap {
      case LessThanOrEqual(attr, v: Long)      => Some(v + 1)
      case LessThan(attr, v: Long)             => Some(v)
      case LessThanOrEqual(attr, v: Timestamp) => Some(v.getTime + 1)
      case LessThan(attr, v: Timestamp)        => Some(v.getTime)
      case _                                   => None
    }.headOption
  }

  def partitions(endpoints: Iterable[HostAndPort], tableName: String, schema: Option[StructType],
                 columnNames: Option[Seq[String]], filters: Array[Filter], tsRangeFieldName: String, readConf: ReadConf): Array[Partition] = {
    val splitCount = readConf.splitCount

    val filtersByAttr = {
      filters
        .map(f => (getFilterAttribute(f), f))
        .filter(f => f._1 != null)
        .groupBy(attrFilter => attrFilter._1)
        .mapValues(a => a.map(p => p._2))
    }

    val rangeFilters = filtersByAttr.get(tsRangeFieldName) match {
      case Some(f) => f
      case None    => throw new IllegalArgumentException(s"No filers found for tsRangeFieldName $tsRangeFieldName")
    }
    val otherFilters = filtersByAttr.filter { case (k, v) => k != tsRangeFieldName }.values.flatten.toArray

    val rangeStart = getMin(rangeFilters) match {
      case Some(minVal) => minVal
      case None         => throw new IllegalArgumentException(s"No GreaterThanOrEqual or GreaterThan filers found for tsRangeFieldName $tsRangeFieldName")
    }

    val rangeEnd = getMax(rangeFilters) match {
      case Some(maxVal) => maxVal
      case None         => throw new IllegalArgumentException(s"No LessThanOrEqual or LessThan filers found for tsRangeFieldName $tsRangeFieldName")
    }

    val timeDiff = (rangeEnd - rangeStart) / (splitCount - 1)
    val partitionsCount = if (timeDiff == 0) 1 else splitCount
    val timePoints = (0 to (partitionsCount - 1)).map(x => rangeStart + x * timeDiff) :+ rangeEnd
    val timeRanges = timePoints zip timePoints.tail

    timeRanges.zipWithIndex.map {
      case (r, indx) =>
        val partitionFilters = GreaterThanOrEqual(tsRangeFieldName, r._1) +: LessThan(tsRangeFieldName, r._2) +: otherFilters
        val where = whereClause(partitionFilters)
        val (sql, values) = toSql(columnNames, tableName, schema, where)
        RiakTSPartition(indx, endpoints, TSQueryData(sql, values))
    }.toArray
  }
}
