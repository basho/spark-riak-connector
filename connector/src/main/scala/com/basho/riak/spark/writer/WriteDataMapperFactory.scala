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
package com.basho.riak.spark.writer

import com.basho.riak.spark.rdd.BucketDef

import scala.reflect.runtime.universe._

trait WriteDataMapper[T] extends Serializable {
  def mapValue(value: T): (String, Any)
}

trait WriteDataMapperFactory[T] {
  def dataMapper(bucket: BucketDef): WriteDataMapper[T]
}

trait LowPriorityWriteDataMapperFactoryImplicits {
  implicit def defaultValueWriterFactory[T]: WriteDataMapperFactory[T] = DefaultWriteDataMapper.factory

  implicit def tuple1Factory[A1: TypeTag]: WriteDataMapperFactory[Tuple1[A1]] =
    TupleWriteDataMapper.factory[Tuple1[A1]]

  implicit def tuple2Factory[A1: TypeTag, A2: TypeTag]: WriteDataMapperFactory[(A1,A2)] =
    TupleWriteDataMapper.factory[(A1,A2)]

  implicit def tuple3Factory[A1: TypeTag, A2: TypeTag, A3: TypeTag]: WriteDataMapperFactory[(A1,A2, A3)] =
    TupleWriteDataMapper.factory[(A1,A2, A3)]

  implicit def tuple4Factory[A1: TypeTag, A2: TypeTag, A3: TypeTag,
  A4: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4)]

  implicit def tuple5Factory[A1: TypeTag, A2: TypeTag, A3: TypeTag,
  A4: TypeTag, A5: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5)]

  implicit def tuple6Factory[A1: TypeTag, A2: TypeTag, A3: TypeTag,
  A4: TypeTag, A5: TypeTag, A6: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6)]

  implicit def tuple7Factory[A1: TypeTag, A2: TypeTag, A3: TypeTag,
  A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7)]

  // scalastyle:off no.whitespace.after.left.bracket
  implicit def tuple8Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag,
  A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8)]

  implicit def tuple9Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9)]

  implicit def tuple10Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10)]

  implicit def tuple11Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag,
  A11: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11)]

  implicit def tuple12Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag,
  A11: TypeTag, A12: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12)]

  implicit def tuple13Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag,
  A13: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13)]

  implicit def tuple14Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag,
  A13: TypeTag, A14: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14)]

  implicit def tuple15Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag,
  A15: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15)]

  implicit def tuple16Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag]: WriteDataMapperFactory[(A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16)]

  implicit def tuple17Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag, A17: TypeTag]: WriteDataMapperFactory[
    (A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16, A17)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17)]

  implicit def tuple18Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag, A17: TypeTag, A18: TypeTag]: WriteDataMapperFactory[
    (A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16, A17, A18)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17,
      A18)]

  implicit def tuple19Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag]: WriteDataMapperFactory[
    (A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16, A17, A18, A19)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17,
      A18, A19)]

  implicit def tuple20Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag]: WriteDataMapperFactory[
    (A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17,
      A18, A19, A20)]

  implicit def tuple21Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag]: WriteDataMapperFactory[
    (A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17,
      A18, A19, A20, A21)]

  implicit def tuple22Factory[
  A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag,
  A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag,
  A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag,
  A22: TypeTag]: WriteDataMapperFactory[
    (A1,A2, A3, A4, A5, A6, A7, A8, A9 ,A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22)] =
    TupleWriteDataMapper.factory[(A1,A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17,
      A18, A19, A20, A21, A22)]
  // scalastyle:on no.whitespace.after.left.bracket
}

object WriteDataMapperFactory extends LowPriorityWriteDataMapperFactoryImplicits
