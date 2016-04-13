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
package com.basho.riak.spark.writer

import org.apache.spark.SparkConf

case class WriteConf(writeReplicas: String = WriteConf.DefaultWriteQuorum, bulkSize: Int = WriteConf.DefaultBulkSize) {

  def overrideProperties(options: Map[String, String]): WriteConf = {
    val newWriteReplicas = options.getOrElse(WriteConf.WriteReplicasProperty, writeReplicas)
    val newBulkSize = options.getOrElse(WriteConf.BulkSizeProperty, bulkSize.toString).toInt
    WriteConf(newWriteReplicas, newBulkSize)
  }
}

object WriteConf {
  val WriteReplicasProperty = "spark.riak.write.replicas"
  val BulkSizeProperty = "spark.riakts.write.bulk-size"

  val DefaultWriteQuorum = "default"
  val DefaultBulkSize = 100

  /** Creates WriteConf based on properties provided to Spark Conf
  *
  * @param conf SparkConf of Spark context with Riak-related properties
  */
  def apply(conf: SparkConf): WriteConf = {
    WriteConf(
      writeReplicas = conf.get(WriteReplicasProperty, DefaultWriteQuorum),
      bulkSize = conf.getInt(BulkSizeProperty, DefaultBulkSize)
    )
  }
  
  /** Creates WriteConf based on an externally provided map of properties
  *   to override those of SparkCon
  *
  * @param conf SparkConf of Spark context to be taken as defaults
  * @param options externally provided map of properties
  */
  def apply(conf: SparkConf, options: Map[String, String]): WriteConf = {
    val writeConf = WriteConf(conf)
    writeConf.overrideProperties(options)
  }
}
