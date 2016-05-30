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

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.writer.{ WriteDataMapper, WriteDataMapperFactory }
import java.util.HashMap

class DefaultWriteDataMapper[T](bucketDef: BucketDef) extends WriteDataMapper[T, KeyValue] {
  override def mapValue(value: T): KeyValue = {
    // scalastyle:off null
    value match {
      // HashMap and Array are used for processing objects comming from python
      // TODO: Move to specific data mappers like in com.basho.riak.spark.writer.mapper.TupleWriteDataMapper
      case m: HashMap[_, _] => {
        if (m.size == 1) {
          val entry = m.entrySet().iterator().next()
          (entry.getKey.toString() -> entry.getValue)
        } else {
          (null, m)
        }
      }
      case a: Array[_] => {
        if (a.size == 1) {
          (null, a.head)
        } else {
          (a.head.toString(), a.tail)
        }
      }
      case _ => (null, value)
    }
    // scalastyle:on null
  }
}

object DefaultWriteDataMapper {
  def factory[T]: WriteDataMapperFactory[T, KeyValue] = new WriteDataMapperFactory[T, KeyValue] {
    override def dataMapper(bucketDef: BucketDef) = {
      new DefaultWriteDataMapper[T](bucketDef)
    }
  }
}
