package com.basho.spark.connector.rdd


import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.spark.connector.query._

import scala.reflect.ClassTag
import scala.language.existentials

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import com.basho.spark.connector.util.{DataConvertingIterator, Logging, CountingIterator}


class RiakRDD[R] private[connector] (
    @transient sc: SparkContext,
    val connector: RiakConnector,
    val bucketType: String,
    val bucketName: String,
    val convert:(Location, RiakObject) => R,
    val query: Query[_] = null,
    val readConf: ReadConf = ReadConf()
    )(
      implicit ct : ClassTag[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  /* Logging classes inheritance conflict fix. */
  override def log = super[Logging].log
  override def logName = super[Logging].logName
  override def logInfo(msg: => String) = super[Logging].logInfo(msg)
  override def logDebug(msg: => String) = super[Logging].logDebug(msg)
  override def logTrace(msg: => String) = super[Logging].logTrace(msg)
  override def logWarning(msg: => String) = super[Logging].logWarning(msg)
  override def logError(msg: => String) = super[Logging].logError(msg)
  override def logInfo(msg: => String, throwable: Throwable) = super[Logging].logInfo(msg, throwable)
  override def logDebug(msg: => String, throwable: Throwable) = super[Logging].logDebug(msg, throwable)
  override def logTrace(msg: => String, throwable: Throwable) = super[Logging].logTrace(msg, throwable)
  override def logWarning(msg: => String, throwable: Throwable) = super[Logging].logWarning(msg, throwable)
  override def logError(msg: => String, throwable: Throwable) = super[Logging].logError(msg, throwable)
  override def isTraceEnabled() = super[Logging].isTraceEnabled

  override def getPartitions: Array[Partition] = {
    val partitions = Array[Partition](RiakPartition(0, connector.hosts, 0))
    logDebug(s"Created total ${partitions.length} partitions for $bucketType.$bucketName.")
    logTrace("Partitions: \n" + partitions.mkString("\n"))
    partitions
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {
    val session = connector.openSession()
    val partition = split.asInstanceOf[RiakPartition]
    val startTime = System.currentTimeMillis()

    if(query == null){
      throw new IllegalStateException("Query is not set")
    }

    val iterator: Iterator[(Location, RiakObject)] = new DataQueryingIterator(query, session)
    val convertingIterator = new DataConvertingIterator[R](iterator, convert)
    val countingIterator = new CountingIterator[R](convertingIterator)
    context.addTaskCompletionListener { (context) =>
      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0
      logDebug(s"Fetched ${countingIterator.count} rows from ${query.bucket} for partition ${partition.index} in $duration%.3f s.")
      session.shutdown()
    }
    countingIterator
  }

  private def copy(
                   query: Query[_] = query,
                   readConf: ReadConf = readConf, connector: RiakConnector = connector): RiakRDD[R] =
    new RiakRDD(sc, connector, bucketType, bucketName, convert, query, readConf)

  def query2iRange(index: String, from: Long, to: Long): RiakRDD[R] = {
    copy(query = new Query2iKeySingleOrRange(BucketDef(bucketType, bucketName), readConf, index, from, Some(to)))
  }

  def query2iKeys[K](index: String, keys: K* ): RiakRDD[R] = {
    copy(query = new Query2iKeys[K](BucketDef(bucketType, bucketName), readConf, index, keys))
  }

  def queryBucketKeys(keys: String*): RiakRDD[R] = {
    copy(query = new QueryBucketKeys(BucketDef(bucketType, bucketName), readConf, keys))
  }
}

object RiakRDD {
  def apply[T](sc: SparkContext, bucketType: String, bucketName: String, convert: (Location, RiakObject) => T)
              (implicit ct: ClassTag[T]): RiakRDD[T] =
    new RiakRDD[T](
      sc, RiakConnector(sc.getConf), bucketType, bucketName, convert)

  def apply[K, V](sc: SparkContext, bucketType: String, bucketName: String, convert: (Location, RiakObject) => (K, V))
                 (implicit keyCT: ClassTag[K], valueCT: ClassTag[V]): RiakRDD[(K, V)] =
    new RiakRDD[(K, V)](
      sc, RiakConnector(sc.getConf), bucketType, bucketName, convert)
}
