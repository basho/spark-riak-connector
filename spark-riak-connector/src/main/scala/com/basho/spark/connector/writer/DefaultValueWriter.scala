package com.basho.spark.connector.writer

import com.basho.spark.connector.rdd.BucketDef

class DefaultValueWriter[T] (bucketDef: BucketDef) extends ValueWriter[T]{
  override def mapValue(value: T): (String, Any) = {
    (null, value)
  }
}

object DefaultValueWriter {
  def factory[T] = new ValueWriterFactory[T] {
    override def valueWriter(bucketDef: BucketDef) = {
      new DefaultValueWriter[T](bucketDef)
    }
  }
}
