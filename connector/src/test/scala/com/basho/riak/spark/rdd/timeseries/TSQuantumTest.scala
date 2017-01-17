/*******************************************************************************
  * Copyright (c) 2016 IBM Corp.
  *
  * Created by Basho Technologies for IBM
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *******************************************************************************/
package com.basho.riak.spark.rdd.timeseries

import com.basho.riak.spark.rdd.RiakTSTests
import org.apache.spark.riak.types.RiakStructType
import org.apache.spark.sql.types._
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[RiakTSTests]))
class TSQuantumTest extends AbstractTimeSeriesTest(false) {

  @Test
  def testCreateSchemaWithQuantum(): Unit = {
    val quantum = 10000L
    val quantizedField = "time"

    val schema = RiakStructType(Seq(
      StructField(name = "surrogate_key", dataType = LongType, nullable = false),
      StructField(name = "family", dataType = StringType, nullable = false),
      StructField(name = quantizedField, dataType = TimestampType, nullable = false),
      StructField(name = "user_id", dataType = StringType, nullable = false),
      StructField(name = "temperature_k", dataType = DoubleType)),
      quantum, quantizedField)

    assert(schema.quantum.nonEmpty)
    assert(schema.quantum.get.equals(quantum))
    assert(schema.quantizedField.nonEmpty)
    assert(schema.quantizedField.get.equals(quantizedField))
  }
}