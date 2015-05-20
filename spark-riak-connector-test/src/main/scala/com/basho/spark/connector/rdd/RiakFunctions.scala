package com.basho.spark.connector.rdd

import java.io.IOException
import java.nio.charset.Charset
import java.util.concurrent.Semaphore

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.cap.Quorum
import com.basho.riak.client.api.commands.kv.{DeleteValue, StoreValue, FetchValue, ListKeys}
import com.basho.riak.client.core.RiakNode.Builder
import com.basho.riak.client.core.query.indexes.{StringBinIndex, LongIntIndex}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.client.core._
import com.basho.riak.client.core.query.{Location, RiakObject, Namespace}
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.{SerializationFeature, DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.net.HostAndPort
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.reflect.ClassTag

case class RiakObjectData( value: Object, key: String, indexes: mutable.Map[String, Object])

trait RiakFunctions{
  protected def riakHosts: Set[HostAndPort]
  protected def numberOfParallelRequests:Int
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  protected val nodeBuilder: RiakNode.Builder

  protected val tolerantMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
    .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .registerModule(DefaultScalaModule)

  def withRiakDo[T](code: RiakClient => T): T = {
    closeRiakSessionAfterUse(createRiakSession()) { session =>
      code(session)
    }
  }

  // Purge data: data might be not only created, but it may be also changed during the previous test case execution
  def createValues(session: RiakClient, ns: Namespace,data: String, purgeBucketBefore: Boolean = false): Unit = {

    val robjects: List[RiakObjectData] = tolerantMapper.readValue(data, new TypeReference[List[RiakObjectData]]{})

    withRiakDo( session => {
      if(purgeBucketBefore){
        foreachKeyInBucket(session, ns, this.deleteByLocation)
      }

      for(i <- 0 until robjects.length) {
        logger.trace(s"Creating value [$i]...")
        val d = robjects.get(i)
        val key = createValue(session, ns, d)
        logger.info(s"Value [$i] was created: key: '$key', ${d.value}")
      }
    })

    // Let's take a nap to be sure that test data was created properly
    Thread.sleep(4000)
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

  def createValue(session: RiakClient, ns: Namespace, objectData: RiakObjectData):String = {
    val obj = new RiakObject()

    var v: String = null

    // Set value
    objectData.value match {
      case map: Map[_,_] =>
        v = tolerantMapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(objectData.value)

        obj.setContentType("application/json")
          .setValue(BinaryValue.create(v))
      case _ =>
        v = objectData.value.toString
        obj.setContentType("text/plain")
          .setValue(BinaryValue.create(v))
    }

    val builder = new StoreValue.Builder(obj)

    // Use provided key, if any
    objectData.key match {
      case k: String =>
        builder.withLocation(new Location(ns, k))
      case _ =>
        builder.withNamespace(ns)
    }

    // Create 2i, if any
    objectData.indexes match {
      case map: mutable.Map[String, _] =>
        val riakIndexes = obj.getIndexes

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

    val store = builder
      .withOption(StoreValue.Option.W, new Quorum(1))
      .build()

    val r = session.execute(store)

    var key = objectData.key
    if(r.hasGeneratedKey) {
      key = r.getGeneratedKey.toStringUtf8
    }
    key
  }

  def foreachKeyInBucket(riakSession: RiakClient, ns: Namespace,  func: (RiakClient, Location) => Boolean): Unit ={
    val req = new ListKeys.Builder(ns).build()
    val response = riakSession.execute(req)

    response.iterator().exists( x=> func(riakSession, x) )
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

  def deleteByLocation(riakSession: RiakClient, location: Location): Boolean ={
    val deleteRequest = new DeleteValue.Builder(location).build()
    riakSession.execute(deleteRequest)
    false
  }

  def deleteByLocationAsync(riakSession: RiakClient, location: Location): RiakFuture[Void,Location] ={
    val deleteRequest = new DeleteValue.Builder(location).build()
    riakSession.executeAsync(deleteRequest)
  }

  def resetAndEmptyBucket(ns:Namespace): Unit = {
    logger.debug(s"Reset bucket '$ns'...")
    val counter = new StatCounter()

    val semaphore = new Semaphore(numberOfParallelRequests)

    val listener = new RiakFutureListener[Void, Location]{
      override def handle(f: RiakFuture[Void, Location]): Unit = {
        if(!f.isSuccess){
          logger.error(s"Can't delete value for location '${f.getQueryInfo}'", f.cause())
        }

        counter.increment()
        semaphore.release()
      }
    }

    withRiakDo(session =>{
      foreachKeyInBucket(session, ns, (client:RiakClient, l:Location) => {
        semaphore.acquire()
        deleteByLocationAsync(client, l).addListener(listener)
        false
      })
    })

    logger.trace("All operations were initiated, waiting for completion")

    // -- wait until completion of all already started operations
    semaphore.acquire(numberOfParallelRequests)
    semaphore.release(numberOfParallelRequests)

    // -- waiting until the bucket become really empty
    var attempts = 6
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
      .dump(s"Bucket '$ns' has been reset. All existed values were removed", logger)
  }

  private def createRiakSession() = {
    val nodes: Set[RiakNode] = riakHosts match {
      case null =>
        Set(nodeBuilder.build())

      case _ =>
        for( host <- riakHosts) yield nodeBuilder
            .withRemoteAddress( host.getHostText)
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

  def apply(hosts:Set[HostAndPort], minConnectionsPerRiakNode:Int = 5) = {
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
