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
package com.basho.spark.connector.util

import com.basho.riak.client.api.convert.JSONConverter
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

object RiakObjectConversionUtil {
  private var mapper: Option[ObjectMapper] = None
  private val classOfRiakObject = classOf[RiakObject]

  private def objectMapper():ObjectMapper ={
    if(mapper.isEmpty){

      /**
       * Need to register Scala module for proper processing of Scala classes
       */
      JSONConverter.registerJacksonModule(DefaultScalaModule)
      mapper = Some(JSONConverter.getObjectMapper)
    }
    mapper.get
  }

  def from[T: ClassTag](location: Location, ro: RiakObject ): T = {
    val ct = implicitly[ClassTag[T]]

    if( ct.runtimeClass == classOfRiakObject){
      // pass through conversion
      ro.asInstanceOf[T]
    } else {
      val (contentType, charset) = parseContentTypeAndCharset(ro.getContentType)

      contentType match {
          case "application/json" if !ct.runtimeClass.equals(classOf[java.lang.String]) =>
            // from https://gist.github.com/rinmalavi/6422520
            objectMapper().readValue[T](ro.getValue.unsafeGetValue(), ct.runtimeClass.asInstanceOf[Class[T]])

          case _ =>
            if (ct.runtimeClass.equals(classOf[java.lang.String])) {
              ro.getValue.toStringUtf8.asInstanceOf[T]
            } else {
              ro.getValue.asInstanceOf[T]
            }
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

  private def parseContentTypeAndCharset(contentType: String): (String,String) ={
    val d = contentType.split(";").transform(x=> x.trim.toLowerCase)

    var charset = "UTF-8"
    for( i <- 1 to d.length-1){
      if(d(i).startsWith("charset")){
        charset = d(i).substring(d(i).indexOf("=") + 1)
      }
    }
    d(0)->charset
  }
}
