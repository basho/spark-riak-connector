package com.basho.spark.connector.writer

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

class TupleValueWriter[T <: Product : TypeTag : ClassTag] extends ValueWriter[T] {
  override def mapValue(value: T): (String, Any) = {
    require( value.productArity > 1)
    val itor = value.productIterator
    val key = itor.next().toString

    if( value.productArity == 2) {
      //to prevent Tuple2._2 serialization as a List of values
      key -> itor.next()
    } else {
      key -> itor
    }
  }
}
