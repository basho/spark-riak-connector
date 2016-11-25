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
package com.basho.riak.spark.util

import com.basho.riak.client.api.convert.{ConverterFactory, JSONConverter}
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

object RiakObjectConversionUtil {

  DataMapper.ensureInitialized()

  private var mapper: Option[ObjectMapper] = None

  private def objectMapper(): ObjectMapper = {
    if (mapper.isEmpty) {
      mapper = Some(JSONConverter.getObjectMapper)
    }
    mapper.get
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
