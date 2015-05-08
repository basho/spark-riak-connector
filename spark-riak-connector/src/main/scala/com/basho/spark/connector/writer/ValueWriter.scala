package com.basho.spark.connector.writer

import com.basho.riak.client.core.query.RiakObject
import com.basho.riak.client.core.util.BinaryValue

trait ValueWriter[T] extends Serializable {
  def mapValue(value: T): (BinaryValue, RiakObject)
}
