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

import com.basho.riak.client.core.query.timeseries.{ColumnDescription, Row}
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.query.TSDataQueryingIterator

import scala.reflect.ClassTag

class DataConvertingIterator[R, S](dataIterator: Iterator[S], convert: (S) => R)
                                  (implicit ct: ClassTag[R]) extends Iterator[R] {
  override def hasNext: Boolean = {
    dataIterator.hasNext
  }

  override def next(): R = {
    val v = dataIterator.next()
    convert(v)
  }
}

object DataConvertingIterator {
  type KV_SOURCE_DATA = (Location, RiakObject)
  type TS_SOURCE_DATA = (Seq[ColumnDescription], TSDataQueryingIterator)

  def createRiakObjectConverting[R](kvIterator: Iterator[KV_SOURCE_DATA], convert: (Location, RiakObject) => R)
                                   (implicit ct: ClassTag[R]): DataConvertingIterator[R, KV_SOURCE_DATA] =
    new DataConvertingIterator[R, KV_SOURCE_DATA](kvIterator, new Function[KV_SOURCE_DATA, R] {
      override def apply(v1: (Location, RiakObject)): R = {
        convert(v1._1, v1._2)
      }
    })

  def createTSConverting[R](tsdata: TS_SOURCE_DATA, convert: (Seq[ColumnDescription], Row) => R)
                           (implicit ct: ClassTag[R]): DataConvertingIterator[R, Row] =
    new DataConvertingIterator[R, Row](tsdata._2, new Function[Row, R] {
      override def apply(v1: Row): R = tsdata match {
        case (Nil, iterator) => convert(iterator.columnDefs, v1)
        case (cds, _) => convert(cds, v1)
      }
    })
}