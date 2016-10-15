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
package com.basho.riak.spark.rdd.connector

import com.basho.riak.client.api.{RiakClient, RiakCommand}
import com.basho.riak.client.core.query.timeseries.TableDefinition
import com.basho.riak.client.core.{FutureOperation, RiakFuture}
import com.basho.riak.spark.rdd.TsTimestampBindingType
import org.apache.spark.sql.types.StructType

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
trait RiakSession extends AutoCloseable {
  def execute[T, S](command: RiakCommand[T, S]): T

  def execute[V, S](operation: FutureOperation[V, _, S]): RiakFuture[V, S]

  def getTableDefinition(name: String, bindingType: TsTimestampBindingType): StructType

  def isClosed: Boolean


  /**
    * @return underlying RiakClient
    * @note was introduced for testing purposes only
    */
  def unwrap(): RiakClient

  /**
    * Minimum number of connections per one RiakNode.
    *
    * It returns minConnection from the first available Riak node,
    * since all Riak Nodes use the same value it should work fine
    */
  def minConnectionsPerNode: Int

  /**
    * Returns maxConnection from the first available Riak node.
    * Since all Riak Nodes use the same value it should work fine
    */
  def maxConnectionsPerNode: Int
}
