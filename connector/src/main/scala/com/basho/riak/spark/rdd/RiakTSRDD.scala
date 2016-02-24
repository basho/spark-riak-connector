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
package com.basho.riak.spark.rdd

import com.basho.riak.client.core.query.timeseries.{Row, ColumnDescription}
import com.basho.riak.spark.query.{TSQueryData, QueryTS}
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.partitioner.{RiakTSPartition, RiakTSPartitioner}
import com.basho.riak.spark.util.{TSConversionUtil, CountingIterator, DataConvertingIterator}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{TaskContext, Partition, Logging, SparkContext}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.1.0
  */
class RiakTSRDD[R] private[spark](
     @transient sc: SparkContext,
     val connector: RiakConnector,
     val bucketName: String,
     val schema:Option[StructType] = None,
     columnNames: Option[Seq[String]] = None,
     whereConstraints: Option[(String, Seq[Any])] = None,
     val query: Option[String] = None,
     val readConf: ReadConf = ReadConf())
    (implicit val ct: ClassTag[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  override def getPartitions: Array[Partition] = {
    // TODO: consider moving sql generation to TSQueryData companion
    var values: Seq[Any] = Seq.empty[Nothing]
    val sql = query.getOrElse(
      "SELECT " +
        (
          columnNames match {
            case None => schema match {
              case Some(s) => s.fieldNames.mkString(", ")
              case None => "*"
            }
            case Some(c: Seq[String]) => c.mkString(", ")
          }
          ) +
        s" FROM $bucketName " +
        (
          whereConstraints match {
            case None => ""
            case Some(x) => x match {
              case (n, vs) =>
                values = vs.map { case (_, v) => v }
                s" WHERE $n"
            }
          }
          )
    )

    RiakTSPartitioner.partitions(connector.hosts, TSQueryData(sql, values))
  }

  private def validateSchema(schema: StructType, columns: Seq[ColumnDescription]): Unit = {
    val columnNames = columns.map(_.getName)

    schema.fieldNames.diff(columnNames).toList match {
      case Nil => columnNames.diff(schema.fieldNames) match {
        case Nil =>
        case diff =>
          throw new IllegalArgumentException(s"Provided schema has nothing about the following fields returned by query: ${diff.mkString(", ")}")
      }
      case diff =>
        throw new IllegalArgumentException(s"Provided schema contains fields that are not returned by query: ${diff.mkString(", ")}")
    }
  }

  private def computeTS(partitionIdx: Int, context: TaskContext, queryData: TSQueryData) = {
    // this implicit values is using to pass parameters to 'com.basho.riak.spark.util.TSConversionUtil$.from'
    implicit val schema = this.schema
    implicit val tsTimestampBinding = readConf.tsTimestampBinding

    val startTime = System.currentTimeMillis()
    val q = new QueryTS(BucketDef(bucketName, bucketName), queryData, readConf)

    val session = connector.openSession()
    val convertingIterator: Iterator[R] = q.nextChunk(session) match {
      case (cds, rows) =>
        if (this.schema.isDefined) validateSchema(schema.get, cds)
        DataConvertingIterator.createTSConverting[R]((cds, rows), TSConversionUtil.from[R])
    }

    val countingIterator = CountingIterator[R](convertingIterator)
    context.addTaskCompletionListener { (context) =>
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logDebug(s"Fetched ${countingIterator.count} rows from ${q.bucket}" +
        f" for partition $partitionIdx in $duration%.3f s.")
      session.close()
    }
    countingIterator
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {

    split match {
      case tsp: RiakTSPartition =>
        computeTS(split.index, context, tsp.queryData)

      case _ =>
        throw new IllegalStateException("Unsupported partition type")
    }
  }

  private def copy(
        query: Option[String] = this.query,
        schema: Option[StructType] = schema,
        columnNames: Option[Seq[String]] = columnNames,
        where: Option[(String, Seq[Any])] = whereConstraints,
        readConf: ReadConf = readConf, connector: RiakConnector = connector): RiakTSRDD[R] =
    new RiakTSRDD(sc, connector, bucketName, schema, columnNames, where, query, readConf)

  def select(columns: String*): RiakTSRDD[R] = {
    copy(columnNames = Some(columns))
  }

  def schema(structType: StructType):RiakTSRDD[R] = {
    copy(schema = Option(structType))
  }

  /**
    * Adds a SQL `WHERE` predicate(s) to the query.
    */
  def where(sql: String, values: Any*): RiakTSRDD[R] = {
    copy(where = Some((sql, values)))
  }

  def sql(query: String): RiakTSRDD[R] = {
    copy(query = Some(query))
  }
}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.1.0
  */
object RiakTSRDD {
  def apply[T](sc: SparkContext, bucketName: String, readConf: ReadConf)
              (implicit ct: ClassTag[T], connector: RiakConnector): RiakTSRDD[T] =
    new RiakTSRDD[T](sc, connector, bucketName, readConf = readConf)

  def apply[T](sc: SparkContext, bucketName: String, readConf: ReadConf, schema: Option[StructType])
              (implicit ct: ClassTag[T], connector: RiakConnector): RiakTSRDD[T] =
    new RiakTSRDD[T](sc, connector, bucketName, schema, readConf = readConf)
}


