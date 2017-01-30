/**
  * Copyright (c) 2016 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.basho.riak.spark.util.python

import java.io.NotSerializableException
import java.io.OutputStream
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.{Collection, UUID, HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArraySeq, Buffer, WrappedArray}
import scala.collection.immutable.HashSet.{HashSet1, HashTrieSet}
import scala.collection.immutable.Map.{Map1, Map2, Map3, Map4, WithDefault}
import scala.collection.immutable.HashMap.{HashMap1, HashTrieMap}
import org.apache.spark.rdd.RDD
import com.basho.riak.spark.util.python.Conversions.asSeq
import net.razorvine.pickle.{ IObjectConstructor, IObjectPickler, Opcodes, Pickler, Unpickler }

import scala.collection.convert.Wrappers.JMapWrapper

class PicklingUtils extends Serializable {
  register()

  def pickler() = {
    register()
    new Pickler()
  }

  def unpickler() = {
    new Unpickler()
  }

  def register() {
    Unpickler.registerConstructor("uuid", "UUID", UUIDUnpickler)

    Pickler.registerCustomPickler(classOf[UUID], UUIDPickler)
    Pickler.registerCustomPickler(classOf[UUIDHolder], UUIDPickler)
    Pickler.registerCustomPickler(Class.forName("scala.collection.immutable.$colon$colon"), ListPickler)
    Pickler.registerCustomPickler(Class.forName("scala.collection.immutable.Nil$"), ListPickler)
    Pickler.registerCustomPickler(Class.forName("scala.collection.immutable.Set$EmptySet$"), ListPickler)
    Pickler.registerCustomPickler(Class.forName("scala.collection.immutable.Map$EmptyMap$"), MapPickler)
    Pickler.registerCustomPickler(classOf[ArraySeq[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Buffer[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[WrappedArray.ofRef[_]], ListPickler)
//    Pickler.registerCustomPickler(classOf[JListWrapper[_]], ListPickler)
//    Pickler.registerCustomPickler(classOf[JSetWrapper[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Tuple1[_]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple2[_, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple3[_, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple4[_, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple5[_, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple6[_, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple7[_, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple8[_, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple9[_, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple10[_, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple11[_, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple12[_, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]], TuplePickler)
    Pickler.registerCustomPickler(classOf[Vector[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set1[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set2[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set3[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[Set.Set4[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[HashSet1[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[HashTrieSet[_]], ListPickler)
    Pickler.registerCustomPickler(classOf[WithDefault[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map1[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map2[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map3[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[Map4[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[HashMap1[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[HashTrieMap[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[JHashMap[_, _]], MapPickler)
    Pickler.registerCustomPickler(classOf[JMapWrapper[_, _]], MapPickler)
  }
}

object PicklingUtils extends PicklingUtils

class PicklableRDD(rdd: RDD[_]) {
  def pickle()(implicit pickling: PicklingUtils) = rdd.mapPartitions(new BatchPickler(), true)
}

class UnpicklableRDD(rdd: RDD[Array[Byte]]) {
  def unpickle()(implicit pickling: PicklingUtils) = rdd.flatMap(new BatchUnpickler())
}

class BatchPickler(batchSize: Int = 1000)(implicit pickling: PicklingUtils)
  extends (Iterator[_] => Iterator[Array[Byte]])
    with Serializable {

  def apply(in: Iterator[_]): Iterator[Array[Byte]] = {
    in.grouped(batchSize).map { b => pickling.pickler().dumps(b.toArray) }
  }
}

class BatchUnpickler(implicit pickling: PicklingUtils) extends (Array[Byte] => Seq[Any]) with Serializable {
  def apply(in: Array[Byte]): Seq[Any] = {
    val unpickled = pickling.unpickler().loads(in)
    asSeq(unpickled)
  }
}

object TuplePickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case p: Product => seqAsJavaList(p.productIterator.toSeq).toArray()
        case _ => throw new NotSerializableException(o.toString())
      })
  }
}

object UUIDPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL)
    out.write("uuid\nUUID\n".getBytes())
    out.write(Opcodes.MARK)
    o match {
      case uuid: UUID => pickler.save(uuid.toString())
      case holder: UUIDHolder => pickler.save(holder.uuid.toString())
    }
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }
}

object UUIDUnpickler extends IObjectConstructor {
  def construct(args: Array[Object]): Object = {
    args.size match {
      case 1 => UUID.fromString(args(0).asInstanceOf[String])
      case _ => new UUIDHolder()
    }
  }
}

class UUIDHolder {
  var uuid: UUID = null

  def __setstate__(values: JHashMap[String, Object]): UUID = {
    val i = values.get("int").asInstanceOf[BigInteger]
    val buffer = ByteBuffer.wrap(i.toByteArray())
    uuid = new UUID(buffer.getLong(), buffer.getLong())
    uuid
  }
}

object ListPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case c: Collection[_] => c
        case b: Buffer[_] => bufferAsJavaList(b)
        case s: Seq[_] => seqAsJavaList(s)
        case p: Product => seqAsJavaList(p.productIterator.toSeq)
        case s: Set[_] => setAsJavaSet(s)
        case _ => throw new NotSerializableException(o.toString())
      })
  }
}

object MapPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    pickler.save(
      o match {
        case m: JMap[_, _] => m
        case m: Map[_, _]  => mapAsJavaMap(m)
      })
  }
}