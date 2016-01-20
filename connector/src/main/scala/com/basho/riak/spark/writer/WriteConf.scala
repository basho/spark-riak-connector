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

case class WriteConf( writeQuorum: Int = WriteConf.DefaultWriteQuorum) {
  
  def overrideProperties(options: Map[String, String]): WriteConf = {
    val newWriteQuorum = options.getOrElse(WriteConf.WriteQuorumProperty, writeQuorum.toString).toInt
    WriteConf(newWriteQuorum)
  }
}

object WriteConf {
  val WriteQuorumProperty = "spark.riak.output.wquorum"

  val DefaultWriteQuorum = 1

  def fromSparkConf(conf: SparkConf): WriteConf = {
    WriteConf(
      writeQuorum = conf.getInt(WriteQuorumProperty, DefaultWriteQuorum)
    )
  }
  
  def fromOptions(options: Map[String, String], conf: SparkConf): WriteConf = {
    val writeConf = fromSparkConf(conf)
    writeConf.overrideProperties(options)
  }
}
