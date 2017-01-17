/*******************************************************************************
 * Copyright (c) 2016 IBM Corp.
 * 
 * Created by Basho Technologies for IBM
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *******************************************************************************/
package org.apache.spark.sql.riak

import com.basho.riak.spark._

import scala.reflect._
import com.basho.riak.spark.rdd.connector.{RiakConnector, RiakConnectorConf}
import com.basho.riak.spark.rdd.{ReadConf, RiakTSRDD}
import com.basho.riak.spark.writer.WriteConf
import com.basho.riak.spark.writer.mapper.SqlDataMapper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.riak.Logging
import org.apache.spark.riak.types.RiakStructType

/**
  * Implements [[BaseRelation]]]], [[InsertableRelation]]]] and [[PrunedFilteredScan]]]]
  * It inserts data to and scans RiakTS bucket.  It pushs down some filters to SQL.
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
private[riak] class RiakRelation(
        bucket: String,
        connector: RiakConnector,
        val readConf: ReadConf,
        val writeConf: WriteConf,
        override val sqlContext: SQLContext,
        userSpecifiedSchema: Option[StructType])
  extends BaseRelation with PrunedFilteredScan with InsertableRelation with Logging {

  override def schema: StructType = userSpecifiedSchema match {
    case Some(st: StructType) => st
    case None => connector withSessionDo { _.getTableDefinition(bucket, readConf.tsTimestampBinding) }
  }

  private[this] val baseRdd: RiakTSRDD[Row] = sqlContext.sparkContext
    .riakTSTable[Row](bucket, readConf, userSpecifiedSchema)(implicitly[ClassTag[Row]], connector)

   def buildScan(): RDD[Row] = baseRdd.asInstanceOf[RDD[Row]]

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val prunedRdd = {
      if (requiredColumns.isEmpty) baseRdd
      else baseRdd.select(requiredColumns: _*)
    }

    val riakStructType = schema match {
      case rst: RiakStructType => rst
      case st: StructType => new RiakStructType(schema.fields)
    }

    val tsRangeFieldName = riakStructType.quantizedField
    if (tsRangeFieldName.nonEmpty && readConf.splitCount > 1) {
      val partitionedRdd = prunedRdd.partitionByTimeRanges(tsRangeFieldName.get, filters)
      riakStructType.quantum match {
        case None    => partitionedRdd
        case Some(q) => partitionedRdd.quantum(q)
      }
    } else {
      prunedRdd.filter(filters)
    }
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    if (overwrite) {
      throw new UnsupportedOperationException("Data truncation is not supported for the moment")
    }

    implicit val rwf = SqlDataMapper.factory[Row]
    implicit val riakConnector = connector
    data.rdd.saveToRiakTS(bucket, writeConf = writeConf)
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
object RiakRelation {
  def apply(bucket: String,
            sqlContext: SQLContext,
            schema: Option[StructType] = None,
            connector: Option[RiakConnector] = None,
            readConf: ReadConf,
            writeConf: WriteConf): RiakRelation = {

    new RiakRelation(bucket, connector.getOrElse(RiakConnector(sqlContext.sparkContext.getConf)),
      readConf, writeConf, sqlContext, schema)
  }

  def apply(sqlContext: SQLContext, parameters: Map[String, String], schema: Option[StructType]): RiakRelation = {
    val existingConf = sqlContext.sparkContext.getConf
    val bucketDef = BucketDef(parameters(DefaultSource.RiakBucketProperty), None)
    val riakConnector = new RiakConnector(RiakConnectorConf(existingConf, parameters))
    val readConf = ReadConf(existingConf, parameters)
    val writeConf = WriteConf(existingConf, parameters)
    RiakRelation(bucketDef.bucket, sqlContext, schema, Some(riakConnector), readConf, writeConf)
  }
}
