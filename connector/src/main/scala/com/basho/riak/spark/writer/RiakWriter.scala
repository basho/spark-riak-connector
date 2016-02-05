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
package com.basho.riak.spark.writer

import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.kv.StoreValue
import com.basho.riak.client.api.convert.JSONConverter
import com.basho.riak.client.core.RiakFuture
import com.basho.riak.client.core.operations.ts.StoreOperation
import com.basho.riak.client.core.query.{Location, Namespace}
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.connector.{RiakSession, RiakConnector}
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.util.CountingIterator
import com.basho.riak.spark.writer.ts.RowDef
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.riak.RiakWriterTaskCompletionListener
import org.apache.spark.{Logging, TaskContext}

import scala.collection.JavaConversions._
import scala.collection._


abstract class RiakWriter[T, U](
                                   connector: RiakConnector,
                                   bucketDef: BucketDef,
                                   dataMapper: WriteDataMapper[T, U],
                                   writeConf: WriteConf) extends Serializable with Logging {

  /** Main entry point */
  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {
    connector.withSessionDo { session =>
      val rowIterator = CountingIterator(data)
      val startTime = System.currentTimeMillis()
      val namespace = new Namespace(bucketDef.bucketType, bucketDef.bucketName)

      logDebug(s"Writing data partition to ${bucketDef.bucketType}.${bucketDef.bucketName}")
      store(session, namespace, rowIterator, dataMapper, writeConf)

      val endTime = System.currentTimeMillis()
      val duration = (endTime - startTime) / 1000.0

      taskContext.addTaskCompletionListener(RiakWriterTaskCompletionListener(rowIterator.count))
    }
  }

  def store(session: RiakSession, ns: Namespace, objects: Iterator[T], dm: WriteDataMapper[T, U], wc: WriteConf): Unit

}

class RiakKVWriter[T](connector: RiakConnector,
                      bucketDef: BucketDef,
                      dataMapper: WriteDataMapper[T, KeyValue],
                      writeConf: WriteConf) extends RiakWriter[T, KeyValue](connector, bucketDef, dataMapper, writeConf) {

  override def store(session: RiakSession, ns: Namespace, objects: Iterator[T],
                     dataMapper: WriteDataMapper[T, KeyValue], writeConf: WriteConf): Unit = {
    val values = objects.map(dataMapper.mapValue)

    /**
      * Since Riak does not provide any Bulk API for storing data (02/01/2015) we will do sequential writes
      */
    values.foreach { case (key, value) =>
      val builder = new StoreValue.Builder(value).withOption(StoreValue.Option.W, new Quorum(writeConf.writeQuorum))

      Option(key) match {
        case None => builder.withNamespace(ns)
        case Some(k) =>
          val location = new Location(ns, k)
          builder.withLocation(location)
      }
      val r = session.execute(builder.build())
      val theRealKey = if (r.hasGeneratedKey) r.getGeneratedKey else key
      logDebug(s"Value was written: '$ns' with key: '$theRealKey': $value")
    }
  }
}

class RiakTSWriter[T](connector: RiakConnector,
                      bucketDef: BucketDef,
                      dataMapper: WriteDataMapper[T, RowDef],
                      writeConf: WriteConf) extends RiakWriter[T, RowDef](connector, bucketDef, dataMapper, writeConf) {
  override def store(session: RiakSession, ns: Namespace, objects: Iterator[T],
                     dataMapper: WriteDataMapper[T, RowDef], writeConf: WriteConf): Unit = {
    val rowDefs = objects.map(dataMapper.mapValue).toList

    val builder = new StoreOperation.Builder(ns.getBucketNameAsString).withRows(rowDefs.map(_.row))
    rowDefs.map(_.columnDescription).filter(_.isDefined).map(_.get).headOption.foreach(descr => builder.withColumns(descr.toList))

    val future: RiakFuture[Void, String] = session.execute(builder.build())
    future.await()

    if (!future.isSuccess) throw future.cause()
  }
}

object RiakWriter {

  /**
    * Need to register Scala module for proper processing of Scala classes
    */
  {
    JSONConverter.registerJacksonModule(DefaultScalaModule)
  }

  def apply[T](
                connector: RiakConnector,
                bucketType: String,
                bucketName: String,
                writeConf: WriteConf)
              (implicit factory: WriteDataMapperFactory[T, KeyValue]): RiakWriter[T, KeyValue] = {

    val bucketDef = new BucketDef(bucketType, bucketName)
    val dataMapper = factory.dataMapper(bucketDef)
    new RiakKVWriter[T](connector, bucketDef, dataMapper, writeConf)
  }

  def apply[T](
                connector: RiakConnector,
                bucketDef: BucketDef,
                writeConf: WriteConf)
              (implicit factory: WriteDataMapperFactory[T, KeyValue]): RiakWriter[T, KeyValue] = {

    val dataMapper = factory.dataMapper(bucketDef)
    new RiakKVWriter[T](connector, bucketDef, dataMapper, writeConf)
  }

  def tsWriter[T](connector: RiakConnector,
                  bucketType: String,
                  bucketName: String,
                  writeConf: WriteConf)
                 (implicit factory: WriteDataMapperFactory[T, RowDef]): RiakWriter[T, RowDef] = {

    val bucketDef = new BucketDef(bucketType, bucketName)
    val dataMapper = factory.dataMapper(bucketDef)
    new RiakTSWriter[T](connector, bucketDef, dataMapper, writeConf)
  }
}
