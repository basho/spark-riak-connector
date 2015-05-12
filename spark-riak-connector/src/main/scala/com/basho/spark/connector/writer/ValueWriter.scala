package com.basho.spark.connector.writer

trait ValueWriter[T] extends Serializable {
  def mapValue(value: T): (String, Any)
}
