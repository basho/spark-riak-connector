/**
  * Copyright (c) 2015-2016 Basho Technologies, Inc.
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
package com.basho.riak

import java.io.IOException

import com.basho.riak.spark.rdd.JsonFunctions
import com.fasterxml.jackson.core.{JsonGenerator, JsonProcessingException, Version}
import com.fasterxml.jackson.databind.{JsonSerializer, ObjectMapper, SerializerProvider}
import net.javacrumbs.jsonunit.JsonAssert
import net.javacrumbs.jsonunit.core.{Configuration, Option => JsonUnitOption}
import com.basho.riak.client.core.query.timeseries.{Cell => RiakCell, Row => RiakRow}
import com.basho.riak.client.core.util.HostAndPort
import com.fasterxml.jackson.databind.module.SimpleModule

import scala.collection.JavaConversions._

trait JsonTestFunctions extends JsonFunctions {

  override protected def tolerantMapper: ObjectMapper = super.tolerantMapper
      .registerModule(
        new SimpleModule("RiakTs Module", new Version(1,0,0,null))
          .addSerializer(classOf[RiakCell], new RiakCellSerializer)
          .addSerializer(classOf[RiakRow], new RiakRowSerializer)
          .addSerializer(classOf[HostAndPort], new HostAndPortSerializer))


  protected def assertEqualsUsingJSON(jsonExpected: AnyRef, actual: AnyRef): Unit = {
    assertEqualsUsingJSONImpl(jsonExpected, actual, null) // scalastyle:ignore
  }

  protected def assertEqualsUsingJSONIgnoreOrder(jsonExpected: AnyRef, actual: AnyRef): Unit = {
    assertEqualsUsingJSONImpl(jsonExpected, actual, JsonAssert.when(JsonUnitOption.IGNORING_ARRAY_ORDER))
  }

  private def assertEqualsUsingJSONImpl(jsonExpected: AnyRef, actual: AnyRef, configuration: Configuration): Unit = {
    var strExpected: String = null // scalastyle:ignore
    var strActual: String = null // scalastyle:ignore
    try {
      strExpected = tolerantMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parseIfString(jsonExpected))
      strActual = tolerantMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parseIfString(actual))
    } catch {
      case ex: JsonProcessingException => throw new RuntimeException(ex)
    }

    scala.Option(configuration) match {
      case Some(x:Configuration) => JsonAssert.assertJsonEquals(strExpected, strActual, x)
      case None => JsonAssert.assertJsonEquals(strExpected, strActual)
    }
  }

  private def parseIfString(raw: AnyRef): Object = {
    raw match {
      case str: String =>
        try {
          tolerantMapper.readValue(str, classOf[java.lang.Object])
        } catch {
          case ex: IOException => throw new RuntimeException(ex)
        }
      case _ => raw
    }
  }

  private class RiakRowSerializer extends JsonSerializer[RiakRow] {
    override def serialize(row: RiakRow, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      if (row == null) {
        jgen.writeNull()
      } else {
        jgen.writeStartArray()
        row.foreach( c=> jgen.writeObject(c))
        jgen.writeEndArray()
      }
    }
  }

  private  class RiakCellSerializer extends JsonSerializer[RiakCell] {
    override def serialize(cell: RiakCell, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
      if (cell == null) {
        jgen.writeNull()
      } else {
        jgen.writeStartObject()
        if (cell.hasBoolean) {
          jgen.writeBooleanField("boolean", cell.getBoolean)
        } else if (cell.hasDouble) {
          jgen.writeNumberField("double", cell.getDouble)
        } else if (cell.hasLong) {
          jgen.writeNumberField("sint64", cell.getLong)
        } else if (cell.hasTimestamp) {
          jgen.writeNumberField("timestamp", cell.getTimestamp)
        } else if (cell.hasVarcharValue) {
          jgen.writeStringField("varchar", cell.getVarcharAsUTF8String)
        } else {
          throw new IllegalStateException("Unsupported cell type")
        }
        jgen.writeEndObject()
      }
    }
  }

  private class HostAndPortSerializer extends JsonSerializer[HostAndPort] {
    override def serialize(value: HostAndPort, jgen: JsonGenerator, provider: SerializerProvider): Unit =
      jgen.writeString(value.getHost + ":" + value.getPort)
  }
}
