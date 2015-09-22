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
package com.basho.riak.spark.rdd

import org.apache.spark.SparkConf

/** RDD read settings
  * @param fetchSize number of keys to fetch in a single round-trip to Riak
  * @param splitCount desired minimum number of Spark partitions to divide the data into
 */
case class ReadConf (
  fetchSize: Int = ReadConf.DefaultFetchSize,
  splitCount: Int = ReadConf.DefaultSplitCount,

  /**
   * DO NOT CHANGE THIS VALUES MANUALLY
   * IT MAY CAUSE PERFORMANCE DEGRADATION
    */
  useStreamingValuesForFBRead: Boolean = ReadConf.DefaultUseStreamingValues4FBRead
)

object ReadConf {
  val DefaultFetchSize = 1000

  // TODO: Need to think about the proper default value
  val DefaultSplitCount = 10

  val DefaultUseStreamingValues4FBRead = true

  def fromSparkConf(conf: SparkConf): ReadConf = {
    ReadConf(
      fetchSize = conf.getInt("spark.riak.input.fetch-size", DefaultFetchSize),
      splitCount = conf.getInt("spark.riak.input.split.count", DefaultSplitCount),
      useStreamingValuesForFBRead = conf.getBoolean("spark.riak.fullbucket.use-streaming-values", DefaultUseStreamingValues4FBRead)
    )
  }
}
