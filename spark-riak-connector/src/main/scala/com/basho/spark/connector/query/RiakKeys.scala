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
package com.basho.spark.connector.query

case class RiakKeys[T](
  keysOrRange: Either[Seq[T],Seq[(T, Option[T])]],
  // Might be None if represents the bucket keys
  index: Option[String] = None
)

object RiakKeys {
  def create2iKeyRanges[K](index: String, ranges: (K, Option[K])*):RiakKeys[K] ={
    new RiakKeys[K](Right(ranges), Some(index))
  }

  def create2iKeys[K](index: String, keys: K*):RiakKeys[K] ={
    new RiakKeys[K](Left(keys), Some(index))
  }

  def createBucketKeys(keys: String*):RiakKeys[String] ={
    new RiakKeys[String](Left(keys))
  }
}
