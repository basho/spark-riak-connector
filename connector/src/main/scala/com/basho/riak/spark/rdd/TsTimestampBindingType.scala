package com.basho.riak.spark.rdd

sealed trait TsTimestampBindingType {
  def value: String
}

case object UseLong extends TsTimestampBindingType {
  override val value = "useLong"
}

case object UseTimestamp extends TsTimestampBindingType {
  override val value = "useTimestamp"
}

object TsTimestampBindingType {
  def apply(value: String): TsTimestampBindingType = value match {
    case UseLong.value => UseLong
    case UseTimestamp.value => UseTimestamp
    case _ =>
      throw new IllegalArgumentException(s"Unknown value '$value' for property 'spark.riakts.bindings.timestamp'")
  }
}
