package com.basho.spark.connector.writer

import com.basho.spark.connector.rdd.BucketDef

class DefaultWriteDataMapper[T] (bucketDef: BucketDef) extends WriteDataMapper[T]{
  override def mapValue(value: T): (String, Any) = {
    (null, value)
  }
}

object DefaultWriteDataMapper {
  def factory[T] = new WriteDataMapperFactory[T] {
    override def dataMapper(bucketDef: BucketDef) = {
      new DefaultWriteDataMapper[T](bucketDef)
    }
  }
}
