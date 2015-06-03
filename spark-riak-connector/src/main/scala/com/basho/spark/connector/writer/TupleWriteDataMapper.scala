package com.basho.spark.connector.writer

import com.basho.spark.connector.rdd.BucketDef

class TupleWriteDataMapper[T <: Product] extends WriteDataMapper[T] {
  override def mapValue(value: T): (String, Any) = {
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
  def factory[T <: Product]: WriteDataMapperFactory[T] = new WriteDataMapperFactory[T] {
    override def dataMapper(bucketDef: BucketDef) = {
      new TupleWriteDataMapper[T]
    }
  }
}
