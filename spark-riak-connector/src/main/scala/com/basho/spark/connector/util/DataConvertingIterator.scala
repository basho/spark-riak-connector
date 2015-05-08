package com.basho.spark.connector.util

import com.basho.riak.client.core.query.{RiakObject, Location}

import scala.reflect.ClassTag

class DataConvertingIterator[R](kvDataIterator: Iterator[(Location,RiakObject)], convert:(Location, RiakObject) => R)
                           (implicit ct : ClassTag[R]) extends Iterator[R]{
  override def hasNext: Boolean = {
    kvDataIterator.hasNext
  }

  override def next(): R = {
    val v = kvDataIterator.next()
    convert( v._1, v._2 )
  }
}
