/**
  * Copyright (c) 2015-2017 Basho Technologies, Inc.
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

import org.apache.spark.{Logging, Partition}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.query.TSQueryData
import com.basho.riak.spark.rdd.{ReadConf, RiakPartition}
import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.api.commands.timeseries.CoveragePlan
import com.basho.riak.spark.rdd.partitioner.PartitioningUtils._
import com.basho.riak.spark.rdd.connector.RiakConnector

import scala.collection.JavaConversions._
import scala.util.control.Exception._
import com.basho.riak.client.core.query.timeseries.CoverageEntry
import com.basho.riak.spark.util.DumpUtils

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
case class RiakTSPartition(
                            index: Int,
                            endpoints: Iterable[HostAndPort],
                            queryData: Seq[TSQueryData]) extends RiakPartition {

  override def dump(lineSep: String = "\n"): String =
    s"[$index] eps: " + DumpUtils.dump(endpoints, ", ") + lineSep +
        s"   queryData (${queryData.size}):" + lineSep + "      " +
        DumpUtils.dump(queryData, lineSep + "      ")
}

trait RiakTSPartitioner {

  protected def interpolateValues(sql: String, values: Seq[Any]): String = {
    val regex = "\\?".r

    def recursiveInterpolateFirst(input: String, iterator: Iterator[Any]): String = iterator.isEmpty match {
      case true =>
        input

      case _ =>
        val rv = iterator.next()

        val v = rv match {
          case ts: Timestamp =>
            ts.getTime.toString

          case s: String =>
            "'" + s + "'"

          case x: Any =>
            x.toString
        }
        recursiveInterpolateFirst(regex.replaceFirstIn(input, v), iterator)
    }
    recursiveInterpolateFirst(sql, values.iterator)
  }

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
                 columnNames: Option[Seq[String]], query: Option[String], whereConstraints: Option[(String, Seq[Any])]): Array[Partition] = {
    val (sql, values) = query match {
      case Some(q) => (q, Seq.empty[Nothing])
      case None => {
        val where = whereConstraints match {
          case Some(w) => w
          case None    => ("", Seq())
        }
        toSql(columnNames, tableName, schema, where)
      }
    }
    val queryString = interpolateValues(sql, values)
    Array(new RiakTSPartition(0, endpoints, Seq(TSQueryData(queryString))))
  }
}

abstract class RangedRiakTSPartitioner(tableName: String, schema: Option[StructType],
                                       columnNames: Option[Seq[String]], filters: Array[Filter], readConf: ReadConf) extends RiakTSPartitioner {

  protected val splitCount = readConf.getOrDefaultSplitCount()

  protected def getFilterAttribute(filter: Filter) = filter match {
    case EqualTo(a, v)            => a
    case GreaterThan(a, v)        => a
    case LessThan(a, v)           => a
    case GreaterThanOrEqual(a, v) => a
    case LessThanOrEqual(a, v)    => a
    case _                        => null
  }

  protected def tsRangeFieldName: String

  protected lazy val filtersByAttr = {
    filters
      .map(f => (getFilterAttribute(f), f))
      .filter(f => f._1 != null)
      .groupBy(attrFilter => attrFilter._1)
      .mapValues(a => a.map(p => p._2))
  }

  protected lazy val rangeFilters: Array[Filter] = {
    filtersByAttr.get(tsRangeFieldName) match {
      case Some(f) => f
      case None    => throw new IllegalArgumentException(s"No filers found for tsRangeFieldName $tsRangeFieldName")
    }
  }

  protected lazy val otherFilters = filtersByAttr.filter { case (k, v) => k != tsRangeFieldName }.values.flatten.toArray

  protected def toTSQueryData(from: Long, lowerInclusive: Boolean, to: Long, upperInclusive: Boolean, coverageEntry: Option[CoverageEntry] = None): TSQueryData = {
    val fromFilter = if (lowerInclusive) GreaterThanOrEqual(tsRangeFieldName, from) else GreaterThan(tsRangeFieldName, from)
    val toFilter = if (upperInclusive) LessThanOrEqual(tsRangeFieldName, to) else LessThan(tsRangeFieldName, to)
    val partitionFilters = fromFilter +: toFilter +: otherFilters
    val where = whereClause(partitionFilters)
    val (sql, values) = toSql(columnNames, tableName, schema, where)
    val queryString = interpolateValues(sql, values)
    TSQueryData(queryString, coverageEntry)
  }

  def partitions(): Array[Partition]
}

object RangedRiakTSPartitioner {
  def apply(connector: RiakConnector, tableName: String, schema: Option[StructType],
            columnNames: Option[Seq[String]], filters: Array[Filter], readConf: ReadConf,
            tsRangeFieldName: String, quantum: Option[Long]): RangedRiakTSPartitioner = {
    new AutomaticRangedRiakTSPartitioner(connector, tableName, schema, columnNames, filters, readConf, tsRangeFieldName, quantum)
  }

  def apply(connector: RiakConnector, tableName: String, schema: Option[StructType],
            columnNames: Option[Seq[String]], filters: Array[Filter], readConf: ReadConf): RangedRiakTSPartitioner = {
    new RiakTSCoveragePlanBasedPartitioner(connector, tableName, schema, columnNames, filters, readConf)
  }
}

/** Splits initial range query into readConf.splitCount number of sub-ranges, each in a separate partition */
class AutomaticRangedRiakTSPartitioner(connector: RiakConnector, tableName: String, schema: Option[StructType],
                                       columnNames: Option[Seq[String]], filters: Array[Filter], readConf: ReadConf,
                                       val tsRangeFieldName: String, quantum: Option[Long]) extends RangedRiakTSPartitioner(tableName, schema, columnNames, filters, readConf) {

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

  lazy val timeRanges: Seq[Seq[(Long, Long)]] = {
    val rangeStart = getMin(rangeFilters) match {
      case Some(minVal) => minVal
      case None         => throw new IllegalArgumentException(s"No GreaterThanOrEqual or GreaterThan filers found for tsRangeFieldName $tsRangeFieldName")
    }

    val rangeEnd = getMax(rangeFilters) match {
      case Some(maxVal) => maxVal
      case None         => throw new IllegalArgumentException(s"No LessThanOrEqual or LessThan filers found for tsRangeFieldName $tsRangeFieldName")
    }

    val initialRangePeriod = rangeEnd - rangeStart
    require(initialRangePeriod > 0, "Invalid range query")
    val timeDiff = (initialRangePeriod) / splitCount
    val numberOfPartitions = if (timeDiff == 0) 1 else splitCount

    val numberOfRanges = quantum match {
      case Some(q) if (timeDiff > quantaLimit * q) => Math.ceil(initialRangePeriod.toDouble / (quantaLimit * q).toDouble).toInt
      case _ => if (timeDiff == 0) 1 else splitCount // last partition with rangeEnd will be included later
    }

    val evenDistributionOfRange = distributeEvenly(initialRangePeriod, numberOfRanges).toList
    val timeRanges = createRanges(rangeStart, evenDistributionOfRange)

    // group ranges to create numberOfpartitions partitions so that a partition has Sequence of ranges
    val evenDistributionBetweenPartitions = distributeEvenly(timeRanges.size, numberOfPartitions).toList
    val rangesInPartition = splitListIntoGroupes(timeRanges, evenDistributionBetweenPartitions)

    rangesInPartition
  }

  private def createRanges(start: Long, distrList: List[Long]): Seq[(Long, Long)] = {
    distrList match {
      case Nil => Nil
      case x :: xs => {
        val end = (start + x)
        (start, end) +: createRanges(end, xs)
      }
    }
  }

  override def partitions(): Array[Partition] = {
    timeRanges.zipWithIndex.map {
      case (ranges, indx) => val queryData = ranges.map(range => buildQuery(range._1, range._2))
        RiakTSPartition(indx, connector.hosts, queryData).asInstanceOf[Partition]
    }.toArray
  }

  private def buildQuery(from: Long, to: Long) = {
    val rangeFieldFilter: Array[Filter] = if (from == to)
      Array(GreaterThanOrEqual(tsRangeFieldName, from), LessThanOrEqual(tsRangeFieldName, to))
    else
      Array(GreaterThanOrEqual(tsRangeFieldName, from), LessThan(tsRangeFieldName, to))
    val partitionFilters = rangeFieldFilter ++ otherFilters
    val where = whereClause(partitionFilters)
    val (sql, values) = toSql(columnNames, tableName, schema, where)
    val queryString = interpolateValues(sql, values)
    TSQueryData(queryString)
  }
}

class RiakTSCoveragePlanBasedPartitioner(connector: RiakConnector, tableName: String, schema: Option[StructType],
                                         columnNames: Option[Seq[String]], filters: Array[Filter], readConf: ReadConf) extends RangedRiakTSPartitioner(tableName, schema, columnNames, filters, readConf)
  with Logging {

  val where = whereClause(filters)
  val (queryRaw, vals) = toSql(columnNames, tableName, schema, where)
  val query = interpolateValues(queryRaw, vals)

  lazy val coveragePlan = connector.withSessionDo(session => {

    val cmd = new CoveragePlan.Builder(tableName, query).build()

    allCatch either session.execute(cmd) match {
      case Right(cp) => cp
      case Left(ex) => {
        if (ex.getCause.isInstanceOf[RiakResponseException] && ex.getCause.getMessage.equals("Unknown message code: 70")) {
          throw new IllegalStateException("Full bucket read is not supported on your version of Riak", ex.getCause)
        } else throw ex
      }
    }
  })

  override lazy val tsRangeFieldName = coveragePlan.head.getFieldName

  override def partitions(): Array[Partition] = {
    val hosts = coveragePlan.hosts

    require(splitCount >= hosts.size)
    val coverageEntriesCount = coveragePlan.size
    val partitionsCount = if (splitCount <= coverageEntriesCount) splitCount else coverageEntriesCount

    if (log.isTraceEnabled()) {
      logTrace("\n----------------------------------------\n" +
        s" [Auto TS Partitioner]  Requested: split up to $splitCount partitions\n" +
        s"                        Actual: the only $partitionsCount partitions might be created\n" +
        "--\n" +
        s"Coverage plan ($coverageEntriesCount coverage entries):\n" +
        DumpUtils.dumpWithIdx(coveragePlan, "\n  ") +
        "\n----------------------------------------\n")
    }

    val evenPartitionDistributionBetweenHosts = distributeEvenly(partitionsCount, hosts.size)

    if (log.isTraceEnabled()) {
      log.trace(s"evenPartitionDistributionBetweenHosts: ${evenPartitionDistributionBetweenHosts.mkString(",")}")
    }

    val numberOfEntriesInPartitionPerHost =
      (hosts zip evenPartitionDistributionBetweenHosts) flatMap {
        case (h, num) =>
          splitListEvenly(coveragePlan.hostEntries(h), num) map{(h, _)}
      }

    val partitions = for {
      ((host, coverageEntries), partitionIdx) <- numberOfEntriesInPartitionPerHost.zipWithIndex
      tsQueryData = coverageEntries.map(ce => toTSQueryData(ce.getLowerBound, ce.isLowerBoundInclusive, ce.getUpperBound, ce.isUpperBoundInclusive, Some(ce)))
      partition = RiakTSPartition(partitionIdx, hosts.toSet, tsQueryData)
    } yield partition

    val result = partitions.toArray

    if (log.isDebugEnabled()) {
      logDebug("\n----------------------------------------\n" +
        s" [Auto TS Partitioner]  Requested: split up to $splitCount partitions\n" +
        s"                        Actual: the created partitions are:\n" +
        "--\n" +
        DumpUtils.dump(result, "\n") +
        "\n----------------------------------------\n")
    }

    // Double check that all coverage entries were used
    val numberOfUsedCoverageEntries = partitions.foldLeft(0){ (sum, p) => sum + p.queryData.size}
    require( numberOfUsedCoverageEntries == coverageEntriesCount)

    result.asInstanceOf[Array[Partition]]
  }
}
