package com.basho.spark.connector.writer

import com.basho.riak.client.api.convert.JSONConverter
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection._
import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.kv.StoreValue
import com.basho.spark.connector.rdd.{BucketDef, RiakConnector}
import com.basho.spark.connector.util.CountingIterator
import org.apache.spark.{Logging, TaskContext}
import com.basho.riak.client.core.query.{Location, Namespace}


class BucketWriter[T] private (
    connector: RiakConnector,
    bucketDef: BucketDef,
    dataMapper: WriteDataMapper[T],
    writeConf: WriteConf) extends Serializable with Logging {

  private val vw: WriteDataMapper[T] = dataMapper

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
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

    // scalastyle:off null
    if( obj._1 == null) {
      builder.withNamespace(ns)
    }else {
      val location = new Location(ns, obj._1)
      builder.withLocation(location)
    }
    // scalastyle:on null

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

  def apply[T: WriteDataMapperFactory](
      connector: RiakConnector,
      bucketType: String,
      bucketName: String,
      writeConf: WriteConf): BucketWriter[T] = {

    val bucketDef = new BucketDef(bucketType, bucketName)
    val dataMapper = implicitly[WriteDataMapperFactory[T]].dataMapper(bucketDef)

    new BucketWriter[T](connector, bucketDef, dataMapper, writeConf)
  }

  def apply[T: WriteDataMapperFactory](
      connector: RiakConnector,
      bucketDef: BucketDef,
      writeConf: WriteConf): BucketWriter[T] = {

    val valueWriter = implicitly[WriteDataMapperFactory[T]].dataMapper(bucketDef)
    new BucketWriter[T](connector, bucketDef, valueWriter, writeConf)
  }
}
