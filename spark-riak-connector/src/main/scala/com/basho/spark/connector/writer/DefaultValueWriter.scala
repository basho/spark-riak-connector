package com.basho.spark.connector.writer

import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.util.BinaryValue
import com.basho.spark.connector.rdd.BucketDef
import com.basho.spark.connector.util.RiakObjectConversionUtil

class DefaultValueWriter[T] (bucketDef: BucketDef) extends ValueWriter[T]{
  override def mapValue(value: T): (BinaryValue, RiakObject) = {
    val obj = RiakObjectConversionUtil.to(value)
    (null, obj)
  }
}

object DefaultValueWriter {
  def factory[T] = new ValueWriterFactory[T] {
    override def valueWriter(bucketDef: BucketDef) = {
      new DefaultValueWriter[T](bucketDef)
    }
  }
}
