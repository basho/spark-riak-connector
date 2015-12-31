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
package org.apache.spark.sql.riak

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.{ReadConf, RiakConnector, RiakTSRDD}
import com.basho.riak.spark.util.TimeSeriesToSparkSqlConversion
import com.basho.riak.spark.writer.WriteConf
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, sources}
import scala.collection.convert.decorateAsScala._

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
private[riak] class RiakRelation(
        bucket: String,
        connector: RiakConnector,
        readConf: ReadConf,
        writeConf: WriteConf,
        override val sqlContext: SQLContext,
        userSpecifiedSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan with Logging {

  override def schema: StructType = userSpecifiedSchema match {
    case None =>
       // -- get schema from Riak TS
      connector.withSessionDo(session => {
        val request = new com.basho.riak.client.api.commands.timeseries.Query.Builder(s"DESCRIBE $bucket")
                        .build()

        val response = session.execute(request)

        require(
          "Column".equals(response.getColumnDescriptions.get(0).getName)
            && "Type".equals(response.getColumnDescriptions.get(1).getName)
            && "Is Null".equals(response.getColumnDescriptions.get(2).getName),
          "Describe response has unexpected fields order or it has unexpected format"
        )

        val fields = for {r: com.basho.riak.client.core.query.timeseries.Row <- response.getRows.asScala } yield {
          val cells = r.getCells
          val name: String = cells.get(0).getVarcharAsUTF8String
          val t: DataType = TimeSeriesToSparkSqlConversion.asDataType(cells.get(1).getVarcharAsUTF8String)
          val nullable: Boolean = cells.get(2).getBoolean

          StructField(name, t, nullable)
        }
        StructType(fields)
      })

    case Some(st: StructType) => st
  }

  private[this] val baseRdd: RiakTSRDD[Row] =
    sqlContext.sparkContext.riakTSBucket[Row](bucket)

  def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val prunedRdd = requiredColumns.isEmpty match {
      case false => baseRdd.select(requiredColumns: _*)
      case _ => baseRdd
    }

    filters.isEmpty match {
      case false =>
        logInfo(s"filters: ${filters.mkString(", ")}")

        whereClause(filters) match {
          case (sql, values) /*if values.nonEmpty*/ => prunedRdd.where(sql, values: _*)
          case _ => prunedRdd
        }
      case _ => prunedRdd
    }
  }

  /** Construct Sql clause */
  private def filterToSqlAndValue(filter: Any): (String, Any) = {
    val (attribute, sqlOperator, value) = filter match {
      case sources.EqualTo(a, v) => (a, "=", v)
      case sources.LessThan(a, v) => (a, "<", v)
      case sources.LessThanOrEqual(a, v) => (a, "<=", v)
      case sources.GreaterThan(a, v) => (a, ">", v)
      case sources.GreaterThanOrEqual(a, v) => (a, ">=", v)
      case _ =>
        throw new UnsupportedOperationException(
          s"It's not a valid filter $filter to be pushed down, only >, <, >=, <=  are allowed.")
    }

    // TODO: need to add pattern matching for values, to be sure that they are used correctly
    (s"$attribute $sqlOperator ?", value)
  }

  private def whereClause(pushdownFilters: Seq[Any]): (String, Seq[Any]) = {
    val sqlValue = pushdownFilters.map(filterToSqlAndValue)
    val sql = sqlValue.map(_._1).mkString(" AND ")
    val args = sqlValue.map(_._2)
    (sql, sqlValue.seq)
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
object RiakRelation {
  def apply(
             bucket: String,
             sqlContext: SQLContext,
             schema: Option[StructType] = None,
             connector: Option[RiakConnector] = None): RiakRelation = {

    new RiakRelation(bucket, connector.getOrElse(RiakConnector(sqlContext.sparkContext.getConf)),
      null, null, sqlContext, schema)
  }
}
