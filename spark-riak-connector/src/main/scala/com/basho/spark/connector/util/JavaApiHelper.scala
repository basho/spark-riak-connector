package com.basho.spark.connector.util

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
