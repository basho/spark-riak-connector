/**
  * Copyright (c) 2016 Basho Technologies, Inc.
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

import com.basho.riak.client.api.convert.JSONConverter
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.Logging

trait DataMapper extends Serializable {
    DataMapper.ensureInitialized()
}

object DataMapper extends Logging {
  private var isInitialized = false

  def ensureInitialized(): Boolean = {
    if (!isInitialized) {
      // Register Scala module to serialize/deserialize Scala stuff smoothly
      JSONConverter.registerJacksonModule(DefaultScalaModule)
      logDebug("Jackson DefaultScalaModule has been registered")
      isInitialized = true
    } else {
      logTrace("Jackson DefaultScalaModule initialization was skipped since module has been registered.")
    }
    isInitialized
  }
}