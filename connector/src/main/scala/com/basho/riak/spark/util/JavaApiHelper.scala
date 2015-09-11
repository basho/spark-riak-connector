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

import scala.reflect._
import scala.reflect.api.{Mirror, TypeCreator, _}
import scala.reflect.runtime.universe._

object JavaApiHelper {

  def mirror: RuntimeMirror = runtimeMirror(Thread.currentThread().getContextClassLoader)

  /** Returns a `TypeTag` for the given class. */
  def getTypeTag[T](clazz: Class[T]): TypeTag[T] = TypeTag.synchronized {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]): U#Type = {
        m.staticClass(clazz.getName).toTypeConstructor
      }
    })
  }

  /** Returns a `TypeTag` for the given class and type parameters. */
  def getTypeTag[T](clazz: Class[_], typeParams: TypeTag[_]*): TypeTag[T] = TypeTag.synchronized {
    TypeTag.apply(mirror, new TypeCreator {
      override def apply[U <: Universe with Singleton](m: Mirror[U]) = {
        val ct = m.staticClass(clazz.getName).toTypeConstructor.asInstanceOf[m.universe.Type]
        val tpt = typeParams.map(_.in(m).tpe.asInstanceOf[m.universe.Type]).toList
        m.universe.appliedType(ct, tpt).asInstanceOf[U#Type]
      }
    })
  }

  /** Returns a `ClassTag` of a given runtime class. */
  def getClassTag[T](clazz: Class[T]): ClassTag[T] = ClassTag(clazz)

  /** Returns a runtime class of a given `TypeTag`. */
  def getRuntimeClass[T](typeTag: TypeTag[T]): Class[T] = mirror.runtimeClass(typeTag.tpe).asInstanceOf[Class[T]]

  /** Returns a runtime class of a given `ClassTag`. */
  def getRuntimeClass[T](classTag: ClassTag[T]): Class[T] = classTag.runtimeClass.asInstanceOf[Class[T]]

  /** Converts an array to a Scala `Seq`. */
  def toScalaSeq[T](array: Array[T]): Seq[T] = array

}
