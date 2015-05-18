package com.basho.spark.connector.rdd

import com.basho.riak.client.core.RiakNode
import com.google.common.net.HostAndPort
import org.slf4j.{LoggerFactory, Logger}

import java.io.IOException

import com.basho.riak.client.core.query.Namespace
import com.fasterxml.jackson.core.JsonProcessingException
import net.javacrumbs.jsonunit.JsonAssert
import net.javacrumbs.jsonunit.core.{Configuration, Option}
import org.junit.Before



abstract class AbstractRiakTest extends RiakFunctions{

  trait RegressionTests {}

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  protected val DEFAULT_NAMESPACE = new Namespace("default","test-bucket")
  protected val DEFAULT_NAMESPACE_4STORE = new Namespace("default", "test-bucket-4store")
  protected val DEFAULT_RIAK_HOST = "localhost"
  protected val DEFAULT_RIAK_PORT = 10017

  protected override val riakHosts:Set[HostAndPort] = null
  protected override val numberOfParallelRequests: Int = 4

  protected override val nodeBuilder: RiakNode.Builder =
    new RiakNode.Builder()
      .withMinConnections(numberOfParallelRequests)
      .withRemoteAddress(DEFAULT_RIAK_HOST)
      .withRemotePort(DEFAULT_RIAK_PORT)

  protected def jsonData(): String = null

  @Before
  def initialize(): Unit ={
    // Purge data: data might be not only created, but it may be also changed during the previous test case execution
    //
    // For manual check: curl -v http://localhost:10018/buckets/test-bucket/keys?keys=true
    List(DEFAULT_NAMESPACE, DEFAULT_NAMESPACE_4STORE) foreach( x=> resetAndEmptyBucket(x))

    withRiakDo( session => {
      val data: String = jsonData()

      if (data != null) {
        createValues(session, DEFAULT_NAMESPACE, data)
      }
    })
  }

  protected def assertEqualsUsingJSON(jsonExpected: AnyRef, actual: AnyRef): Unit = {
    assertEqualsUsingJSONImpl(jsonExpected, actual, null)
  }

  protected def assertEqualsUsingJSONIgnoreOrder(jsonExpected: AnyRef, actual: AnyRef): Unit = {
    assertEqualsUsingJSONImpl(jsonExpected, actual, JsonAssert.when(Option.IGNORING_ARRAY_ORDER))
  }

  private def assertEqualsUsingJSONImpl(jsonExpected: AnyRef, actual: AnyRef, configuration: Configuration) {
    var expected: Object  = null

    jsonExpected match {
      case str: String =>
        try {
          expected = tolerantMapper.readValue(str, classOf[java.lang.Object])
        } catch {
          case ex: IOException => throw new RuntimeException(ex)
        }
      case _ =>
        expected = jsonExpected
    }

    var strExpected: String = null
    var strActual: String = null
    try{
      strExpected = tolerantMapper.writerWithDefaultPrettyPrinter().writeValueAsString(expected)
      strActual = tolerantMapper.writerWithDefaultPrettyPrinter().writeValueAsString(actual)
    }catch{
      case ex: JsonProcessingException => throw new RuntimeException(ex)
    }

    if(configuration != null) {
      JsonAssert.assertJsonEquals(strExpected, strActual, configuration)
    } else {
      JsonAssert.assertJsonEquals(strExpected, strActual)
    }
  }
}
