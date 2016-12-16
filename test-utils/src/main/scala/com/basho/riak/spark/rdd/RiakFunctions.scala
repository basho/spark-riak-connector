/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.rdd

import java.io.IOException
import java.net.InetAddress
import java.nio.charset.Charset
import java.util.concurrent.Semaphore

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.indexes.{IntIndexQuery, BinIndexQuery}
import com.basho.riak.client.api.commands.kv.{DeleteValue, StoreValue, FetchValue, ListKeys}
import com.basho.riak.client.core.RiakNode.Builder
import com.basho.riak.client.core.query.indexes.{StringBinIndex, LongIntIndex}
import com.basho.riak.client.core.util.{HostAndPort, BinaryValue}
import com.basho.riak.client.core._
import com.basho.riak.client.core.query.{Location, RiakObject, Namespace}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{ObjectWriter, SerializationFeature, DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.reflect.ClassTag

case class RiakObjectData( value: Object, key: String, indexes: mutable.Map[String, Object])

//scalastyle:off
trait RiakFunctions extends JsonFunctions {
  protected def riakHosts: Set[HostAndPort]
  protected def numberOfParallelRequests:Int
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  protected val nodeBuilder: RiakNode.Builder

  def withRiakDo[T](code: RiakClient => T): T = {
    closeRiakSessionAfterUse(createRiakSession()) { session =>
      code(session)
    }
  }

  protected def parseRiakObjectData(json: String): List[RiakObjectData] = {
    tolerantMapper.readValue(json, new TypeReference[List[RiakObjectData]]{})
  }


  protected def createRiakObjectFrom(data: AnyRef): (String, RiakObject) = {

    val rod = data match {
      case json:String =>
        tolerantMapper.readValue(json, new TypeReference[RiakObjectData]{})

      case r: RiakObjectData =>
        r
    }

    val ro = new RiakObject()

    var v: String = null

    // Set value
    rod.value match {
      case _: Map[_,_] | _: List[Map[_,_]] =>
        v = tolerantMapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(rod.value)

        ro.setContentType("application/json")
          .setValue(BinaryValue.create(v))
      case _ =>
        v = rod.value.toString
        ro.setContentType("text/plain")
          .setValue(BinaryValue.create(v))
    }


    // Create 2i, if any
    rod.indexes match {
      case map: mutable.Map[String, _] =>
        val riakIndexes = ro.getIndexes

        map foreach( idx => {
          idx._2 match {
            case i: java.lang.Integer =>
              riakIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named(idx._1))
                .add(i.toLong)

            case l: java.lang.Long =>
              riakIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named(idx._1))
                .add(l)

            case str: String =>
              // We need to use this signature (with Charset) because of inconsistency in signatures of StringBinIndex.named()
              riakIndexes.getIndex[StringBinIndex, StringBinIndex.Name](StringBinIndex.named(idx._1, Charset.defaultCharset()))
                .add(str)
          }
        })
      case null =>
      // Indexes aren't provided, here is nothing to do
    }

    rod.key -> ro
  }

  // Purge data: data might be not only created, but it may be also changed during the previous test case execution
  def createValues(session: RiakClient, ns: Namespace,data: String, purgeBucketBefore: Boolean = false): Unit = {

    val rodOjects: List[RiakObjectData] = parseRiakObjectData(data)

    withRiakDo( session => {
      if(purgeBucketBefore){
        foreachKeyInBucket(session, ns, this.deleteByLocation)
      }

      for(i <- 0 until rodOjects.length) {
        logger.trace(s"Creating value [$i]...")
        val (requestedKey, ro) = createRiakObjectFrom(rodOjects.get(i))
        val key = createValueRaw(session, ns, ro, requestedKey, true)
        logger.trace(s"Value [$i] was created: key: '$key', ${rodOjects.get(i).value}")
      }
    })
  }


  def createValuesForBucket(session: RiakClient, bucketName: String, data: String, purgeBucketBefore: Boolean = false): Unit = {
    createValues(session, new Namespace(bucketName), data, purgeBucketBefore)
  }

  def createValueAsync(session: RiakClient, ns: Namespace, obj: AnyRef, key: String = null):RiakFuture[StoreValue.Response, Location] = {
    val builder = new StoreValue.Builder(obj)

    // Use provided key, if any
    key match {
      case k: String =>
        builder.withLocation(new Location(ns, k))
      case _ =>
        builder.withNamespace(ns)
    }

    val store = builder
      .withOption(StoreValue.Option.W, new Quorum(1))
      .build()

    session.executeAsync(store)
  }

  def createValueAsyncForBucket(session: RiakClient, bucketName: String, obj: AnyRef, key: String = null):RiakFuture[StoreValue.Response, Location] = {
    createValueAsync(session, new Namespace(bucketName), obj, key)
  }

  def createValueRaw(session: RiakClient, ns: Namespace, ro: RiakObject, key: String = null, checkCreation: Boolean = true):String = {
    val builder = new StoreValue.Builder(ro)
      .withOption(StoreValue.Option.PW, Quorum.allQuorum())

    // Use provided key, if any
    key match {
      case k: String =>
        builder.withLocation(new Location(ns, k))
      case _ =>
        builder.withNamespace(ns)
    }


    val store = builder
      .withOption(StoreValue.Option.W, new Quorum(1))
      .build()

    val r = session.execute(store)

    var realKey = key
    if(r.hasGeneratedKey) {
      realKey = r.getGeneratedKey.toStringUtf8
    }

    if(checkCreation){
      // To be 100% sure that everything was created properly
      val l = new Location(ns, BinaryValue.create(realKey))
      val conversion = (l: Location, ro: RiakObject) => ro
      for( i <- 6 to 0 by -1)
        try {
          readByLocation[RiakObject](session, l, conversion)
        } catch {
          case e:IllegalStateException if e.getMessage.startsWith("Nothing was found") && i >1 =>{
            logger.debug("Value for '{}' hasn't been created yet", l)
            Thread.sleep(200)
          }
        }

      // Since we proved that value is reachable by key, we expect that it also available by all 2i keys
      ro.getIndexes.foreach( idx => {
        assert(idx.values().size() == 1)
        val name = idx.getName
        val v = idx.values().iterator().next()

        if(!isLocationReachableBy2iKey(session, l, name, v)) {
          throw new IllegalStateException("Location '$l' is not reachable by 2i '$name'")
        }else{
          logger.debug(s"Value for '$l' is reachable by 2i '$name'")
        }
      })
    }
    realKey
  }

  private def isLocationReachableBy2iKey[T](session: RiakClient, location: Location, index: String, key: T): Boolean = {
    val builder = key match {
      case s: String =>
        new BinIndexQuery.Builder(location.getNamespace, index, s)

      case i: Int =>
        new IntIndexQuery.Builder(location.getNamespace, index, i.toLong)

      case l: Long =>
        new IntIndexQuery.Builder(location.getNamespace, index, l)

      case _ =>
        throw new IllegalStateException(s"Type '${key.getClass.getName}' is not suitable for 2i")
    }

    val entries = builder match {
      case iQueryBuilder: IntIndexQuery.Builder =>
        session.execute(iQueryBuilder.build())
          .getEntries

      case bQueryBuilder: BinIndexQuery.Builder =>
        session.execute(bQueryBuilder.build())
          .getEntries
    }

    val locations: Iterable[Location] = entries.map(_.getRiakObjectLocation)
    val c = locations.count( l =>{ location.equals(l)})
    c > 0
  }

  def foreachKeyInBucket(riakSession: RiakClient, ns: Namespace,  func: (RiakClient, Location) => Unit): Unit ={
    val req = new ListKeys.Builder(ns).build()
    val response = riakSession.execute(req)

    response.iterator().foreach( x=> func(riakSession, x) )
  }

  def readByLocation[T:ClassTag](riakSession: RiakClient, location: Location, convert:(Location, RiakObject) => T): T ={
    val fetchRequest = new FetchValue.Builder(location).build()
    val response = riakSession.execute(fetchRequest)

    if( response.isNotFound){
      throw new IllegalStateException(s"Nothing was found for location '$location'")
    }

    if( response.getNumberOfValues > 1){
      throw new IllegalStateException(s"Fetch by Location '$location' returns more than one result: ${response.getNumberOfValues} were actually returned" )
    }

    val ro = response.getValue(classOf[RiakObject])
    convert(location, ro)
  }

  def deleteByLocation(riakSession: RiakClient, location: Location): Unit ={
    val deleteRequest = new DeleteValue.Builder(location).build()
    riakSession.execute(deleteRequest)
  }

  def deleteByLocationAsync(riakSession: RiakClient, location: Location): RiakFuture[Void,Location] ={
    val deleteRequest = new DeleteValue.Builder(location)
      .withOption(DeleteValue.Option.PW, Quorum.allQuorum())
      .withOption(DeleteValue.Option.PR, Quorum.allQuorum())
      .build()

    riakSession.executeAsync(deleteRequest)
  }

  def resetAndEmptyBucketByName(bucketName: String): Unit = resetAndEmptyBucket(new Namespace(bucketName))

  def resetAndEmptyBucket(ns:Namespace): Unit = {
    logger.debug("\n----------\n" +
      "[Bucket RESET]  {}",
      ns)
    val counter = new StatCounter()

    val semaphore = new Semaphore(numberOfParallelRequests)

    val listener = new RiakFutureListener[Void, Location]{
      override def handle(f: RiakFuture[Void, Location]): Unit = {
        try {
          val v = f.get()
          logger.debug("Value was deleted '{}'", f.getQueryInfo)
        }catch{
          case re: RuntimeException =>
            logger.error(s"Can't delete value for '${f.getQueryInfo}'", re)
            throw re

          case e: Exception =>
            logger.error(s"Can't delete value for '${f.getQueryInfo}'", e)
            throw new RuntimeException(e)
        }finally{
          counter.increment()
          semaphore.release()
        }
      }
    }

    withRiakDo(session =>{
      foreachKeyInBucket(session, ns, (client:RiakClient, l:Location) => {
        semaphore.acquire()
        logger.debug("Performing delete for '{}'", l)
        deleteByLocationAsync(client, l).addListener(listener)
      })
    })

    logger.trace("All operations were initiated, waiting for completion")

    // -- wait until completion of all already started operations
    semaphore.acquire(numberOfParallelRequests)
    semaphore.release(numberOfParallelRequests)

    // -- waiting until the bucket become really empty
    var attempts = 10
    var response: ListKeys.Response = null
      withRiakDo(session=>{
        do {
          Thread.sleep(500)
          val req = new ListKeys.Builder(ns).build()
          response = session.execute(req)
          attempts -= 1
        }while(attempts>0 && response.iterator().hasNext)
      })

    response.iterator().toList match {
      case Nil => //Nothing to do
      case l:List[Any] => {
        throw new IllegalStateException(s"Bucket '$ns' is not empty after truncation")
      }
    }

    // --
    counter.stats()
      .dump(s"\n----------\nBucket '$ns' has been reset. All existed values were removed", logger)
  }

  private def createRiakSession() = {
    val nodes: Set[RiakNode] = riakHosts match {
      case null =>
        Set(nodeBuilder.build())

      case _ =>
        for( host <- riakHosts) yield nodeBuilder
            .withRemoteAddress( host.getHost)
            .withRemotePort(host.getPortOrDefault(8087))
            .build()

    }

    try {
      val cluster = RiakCluster.builder(
        nodes.toList
      ).build()

      cluster.start()

      new RiakClient(cluster)
    } catch {
      case e: Throwable =>
        throw new IOException(
          s"Failed to connect to Riak", e)
    }
  }

  private def closeRiakSessionAfterUse[T](closeable: RiakClient)(code: RiakClient => T): T =
    try code(closeable) finally {
      closeable.shutdown()
    }
}

object RiakFunctions {

  private def minConnections(nb: RiakNode.Builder):Int ={
    val f = nb.getClass.getDeclaredField("minConnections")
    f.setAccessible(true)
    f.get(nb).asInstanceOf[Int]
  }

  def apply(conf: SparkConf): RiakFunctions = {
    val hostsStr = conf.get("spark.riak.connection.host", InetAddress.getLocalHost.getHostAddress)
    val hosts = HostAndPort.hostsFromString(hostsStr, 8087).toSet
    val minConnections = conf.get("spark.riak.connections.min", "5").toInt
    val maxConnections = conf.get("spark.riak.connections.max", "15").toInt

    apply(hosts, minConnections, maxConnections)
  }

  def apply(hosts:Set[HostAndPort], minConnectionsPerRiakNode:Int = 5, maxConnectionsPerRiakNode: Int = 15) = {
    val nb = new Builder()
      .withMinConnections(minConnectionsPerRiakNode)


    new RiakFunctions {
      override protected val riakHosts:Set[HostAndPort] = hosts
      override protected val nodeBuilder: Builder = nb
      override protected val numberOfParallelRequests: Int = minConnections(nb)
    }
  }

  def apply(nb: RiakNode.Builder):RiakFunctions = {
    new RiakFunctions {
      override protected val riakHosts:Set[HostAndPort] = null
      override protected val nodeBuilder: Builder = nb
      override val numberOfParallelRequests: Int = minConnections(nb)
    }
  }
}


trait JsonFunctions {
  protected def tolerantMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .registerModule(DefaultScalaModule)

  def asStrictJSON(data: Any, prettyPrint: Boolean = false): String = {
    val writter: ObjectWriter = prettyPrint match {
      case true =>
        tolerantMapper.writerWithDefaultPrettyPrinter()
      case _ =>
        tolerantMapper.writer()
    }

    writter.writeValueAsString(
      data match {
        case s: String =>
          tolerantMapper.readValue(s, classOf[Object])
        case _ => data
      }
    )
  }
}