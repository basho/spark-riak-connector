package com.basho.spark.connector.util

import com.basho.riak.client.api.convert.JSONConverter
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

object RiakObjectConversionUtil {
  private var mapper: ObjectMapper = null

  private def objectMapper():ObjectMapper ={
    if(mapper == null){

      /**
       * Need to register Scala module for proper processing of Scala classes
       */
      JSONConverter.registerJacksonModule(DefaultScalaModule)
      mapper = JSONConverter.getObjectMapper
    }
    mapper
  }

  private def getClassTag[T : ClassTag]: ClassTag[T] = implicitly[ClassTag[T]]

  def from[T: ClassTag](location: Location, ro: RiakObject ): T = {
    val ct = getClassTag[T]

    ro.getContentType match {
      case "application/json" if !ct.runtimeClass.equals(classOf[java.lang.String]) =>
        // from https://gist.github.com/rinmalavi/6422520
        objectMapper().readValue[T](ro.getValue.unsafeGetValue(), ct.runtimeClass.asInstanceOf[Class[T]])

      case _ =>
        if(ct.runtimeClass.equals(classOf[java.lang.String])){
          ro.getValue.toStringUtf8.asInstanceOf[T]
        } else {
          ro.getValue.asInstanceOf[T]
        }
    }
  }

  def to[T](value: T): RiakObject = {
    // TODO: we need to think about smarter approach to handle primitive types such as int, long, etc.
    value match {
      case s: String =>
        new RiakObject()
          .setContentType("text/plain")
          .setValue(BinaryValue.create(value.asInstanceOf[String]))
      case _ =>
        // value as a strict JSON
        val v = objectMapper().writeValueAsString(value)

        new RiakObject()
          .setContentType("application/json")
          .setValue(BinaryValue.create(v))
    }
  }
}
