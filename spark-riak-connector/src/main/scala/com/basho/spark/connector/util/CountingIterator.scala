package com.basho.spark.connector.util

class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
  private var _count = 0

  /** Returns the number of successful invocations of `next` */
  def count: Int = _count

  def hasNext: Boolean = iterator.hasNext

  def next(): T = {
    val item = iterator.next()
    _count += 1
    item
  }
}
