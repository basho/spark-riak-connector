package com.basho.spark.connector.writer

import com.basho.riak.client.api.convert.JSONConverter
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection._
import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.kv.StoreValue
import com.basho.spark.connector.rdd.{BucketDef, RiakConnector}
import com.basho.spark.connector.util.{CountingIterator, Logging}
import org.apache.spark.TaskContext
import com.basho.riak.client.core.query.{Location, Namespace}


class BucketWriter[T] private (
    connector: RiakConnector,
    bucketDef: BucketDef,
    valueWriter: ValueWriter[T],
    writeConf: WriteConf) extends Serializable with Logging {

  private val vw: ValueWriter[T] = valueWriter

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]) {
    connector.withSessionDo { session =>
      val rowIterator = new CountingIterator(data)
      val startTime = System.currentTimeMillis()
      val namespace = new Namespace(bucketDef.bucketType, bucketDef.bucketName)

      logDebug(s"Writing data partition to ${bucketDef.bucketType}.${bucketDef.bucketName}")

      /**
       * Since Riak does not provide any Bulk API for storing data (02/01/20015) we will do sequential writes
       */
      rowIterator.foreach(x => storeObject(session, namespace, x))

      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0

      logInfo(f"Wrote ${rowIterator.count} rows to ${bucketDef.bucketType}.${bucketDef.bucketName} in $duration%.3f s.")
    }
  }

  private def storeObject(session: RiakClient, ns: Namespace, value: T): Unit ={
    val obj:(String, Any)  = vw.mapValue(value)

    val builder = new StoreValue.Builder(obj._2).withOption(StoreValue.Option.W, new Quorum(writeConf.writeQuorum))

    if( obj._1 == null) {
      builder.withNamespace(ns)
    }else {
      val location = new Location(ns, obj._1)
      builder.withLocation(location)
    }

    val r = session.execute(builder.build())
    val theRealKey = if (r.hasGeneratedKey) r.getGeneratedKey else obj._1
    logInfo(s"Value was created: '$ns' with key: '$theRealKey': ${obj._2}")
  }
}

object BucketWriter {

  /**
   * Need to register Scala module for proper processing of Scala classes
   */
  {
    JSONConverter.registerJacksonModule(DefaultScalaModule)
  }

  def apply[T: ValueWriterFactory](
      connector: RiakConnector,
      bucketType: String,
      bucketName: String,
      writeConf: WriteConf): BucketWriter[T] = {

    val bucketDef = new BucketDef(bucketType, bucketName)
    val valueWriter = implicitly[ValueWriterFactory[T]].valueWriter(bucketDef)

    new BucketWriter[T](connector, bucketDef, valueWriter, writeConf)
  }

  def apply[T: ValueWriterFactory](
      connector: RiakConnector,
      bucketDef: BucketDef,
      writeConf: WriteConf): BucketWriter[T] = {

    val valueWriter = implicitly[ValueWriterFactory[T]].valueWriter(bucketDef)
    new BucketWriter[T](connector, bucketDef, valueWriter, writeConf)
  }
}