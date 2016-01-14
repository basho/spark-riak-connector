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
import com.basho.riak.spark.rdd.partitioner.{RiakTSPartition, RiakTSPartitioner}
import com.basho.riak.spark.util.{TSConversionUtil, CountingIterator, DataConvertingIterator}
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
            case None => "*"
            case Some(c: Seq[String]) => c.mkString(", ")
          }
          ) +
        s" FROM $bucketName " +
        (
          whereConstraints match {
            case None => ""
            case Some(x: (String, Seq[(String, Any)])) =>
              values = x._2.map(k => k._2)
              s" WHERE ${x._1}"
          }
          )
    )

    RiakTSPartitioner.partitions(connector.hosts, TSQueryData(sql, values))
  }

  private def computeTS(partitionIdx: Int, context: TaskContext, queryData: TSQueryData) = {
    val startTime = System.currentTimeMillis()
    val q = new QueryTS(BucketDef(bucketName, bucketName), queryData, readConf)

    val session = connector.openSession()
    val result: (Seq[ColumnDescription], Seq[Row]) = q.nextChunk(session)

    val convertingIterator = DataConvertingIterator.createTSConverting[R](result, TSConversionUtil.from[R])
    val countingIterator = CountingIterator[R](convertingIterator)
    context.addTaskCompletionListener { (context) =>
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logDebug(s"Fetched ${countingIterator.count} rows from ${q.bucket}" +
        f" for partition $partitionIdx in $duration%.3f s.")
      session.shutdown()
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
        columnNames: Option[Seq[String]] = columnNames,
        where: Option[(String, Seq[Any])] = whereConstraints,
        readConf: ReadConf = readConf, connector: RiakConnector = connector): RiakTSRDD[R] =
    new RiakTSRDD(sc, connector, bucketName, columnNames, where, query, readConf)

  def select(columns: String*): RiakTSRDD[R] = {
    copy(columnNames = Some(columns))
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
  def apply[T](sc: SparkContext, bucketName: String)
              (implicit ct: ClassTag[T]): RiakTSRDD[T] =
    new RiakTSRDD[T](
      sc, RiakConnector(sc.getConf), bucketName)
}


