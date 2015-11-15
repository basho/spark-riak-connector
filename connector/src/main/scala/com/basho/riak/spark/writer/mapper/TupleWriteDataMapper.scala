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
package com.basho.riak.spark.writer.mapper

import com.basho.riak.spark.KeyValue
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.writer.{WriteDataMapper, WriteDataMapperFactory}

class TupleWriteDataMapper[T <: Product] extends WriteDataMapper[T, KeyValue] {
  override def mapValue(value: T): KeyValue = {
    val itor = value.productIterator

    if(value.productArity == 1){
      // scalastyle:off null
      (null, itor.next())
      // scalastyle:on null
    } else {
      val key = itor.next().toString

      if (value.productArity == 2) {
        // to prevent Tuple2._2 serialization as a List of values
        key -> itor.next()
      } else {
        key -> itor
      }
    }
  }
}

object TupleWriteDataMapper{
  def factory[T <: Product]: WriteDataMapperFactory[T, KeyValue] = new WriteDataMapperFactory[T, KeyValue] {
    override def dataMapper(bucketDef: BucketDef) = {
      new TupleWriteDataMapper[T]
    }
  }
}
