/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.util

class CountingIterator[T](iterator: Iterator[T]) extends Iterator[T] {
  private var _count = 0

  /** Returns the number of successful invocations of `next` */
  def count: Int = _count

  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = {
    val item = iterator.next()
    _count += 1
    item
  }
}

object CountingIterator {
  def apply[T](iterator: Iterator[T]):CountingIterator[T] = new CountingIterator[T](iterator)
}
