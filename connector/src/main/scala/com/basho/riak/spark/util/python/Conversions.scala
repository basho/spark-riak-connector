/**
  * Copyright (c) 2016 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.basho.riak.spark.util.python

import java.util.{List => JList}
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import java.util.{List => JList}

object Conversions {
  def asArray[T: ClassTag](c: Any): Array[T] = c match {
    case a: Array[T] => a
    case b: Buffer[T] => b.toArray
    case l: List[T] => l.toArray
    case l: JList[T] => asScalaBuffer(l).toArray
    case _ => throw new IllegalArgumentException(c.getClass() + " can't be converted to an Array")
  }

  def asSeq[T: ClassTag](c: Any): Seq[T] = c match {
    case a: Array[T] => a
    case b: Buffer[T] => b
    case l: List[T] => l
    case l: JList[T] => asScalaBuffer(l).toSeq
    case _ => throw new IllegalArgumentException(c.getClass() + " can't be converted to a Seq")
  }

}