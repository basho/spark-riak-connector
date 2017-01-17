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

import java.io.InputStream
import java.util.Properties
import org.apache.spark.SparkConf
import java.time.Duration
import org.joda.time.Duration
import org.apache.spark.network.util.JavaUtils

/** RDD read settings
 *
  * @param fetchSize number of keys to fetch in a single round-trip to Riak
  * @param splitCount desired minimum number of Spark partitions to divide the data into
 */
case class ReadConf (
  val fetchSize: Int = ReadConf.DefaultFetchSize,
  val splitCount: Int = ReadConf.DefaultSplitCount,
  val tsTimestampBinding: TsTimestampBindingType = ReadConf.DefaultTsTimestampBinding,
  /**
   * Used only in ranged partitioner to identify quantized field.
   * Usage example:
   *    sparkSession.read
   *      .option("spark.riak.partitioning.ts-range-field-name", "time")
   * Providing this property automatically turns on RangedRiakTSPartitioner
   */
  val tsRangeFieldName: String = null,
  val quantum: Option[Long] = None,

  /**
    * Turns on streaming values support for PEx.
    *
    * It will make Full Bucket Reads more efficient: values will be streamed as a part of the FBR response
    * instead of being fetched in a separate operations.
    *
    * Supported only by EE
    *
    * DO NOT CHANGE THIS VALUES MANUALLY IF YOU DON'T KNOW WHAT YOU ARE DOING
    * IT MAY CAUSE EITHER PERFORMANCE DEGRADATION or INTRODUCE FBR ERRORS
    */
  useStreamingValuesForFBRead: Boolean = ReadConf.DefaultUseStreamingValues4FBRead
) {

  def overrideProperties(options: Map[String, String]): ReadConf = {
    val newFetchSize = options.getOrElse(ReadConf.fetchSizePropName, fetchSize.toString).toInt
    val newSplitCount = options.getOrElse(ReadConf.splitCountPropName, splitCount.toString).toInt
    val newUseStreamingValuesForFBRead = options.getOrElse(ReadConf.useStreamingValuesPropName, useStreamingValuesForFBRead.toString).toBoolean
    val newTsTimestampBinding = TsTimestampBindingType(options.getOrElse(ReadConf.tsBindingsTimestamp, tsTimestampBinding.value))
    val newTsRangeFieldName = options.getOrElse(ReadConf.tsRangeFieldPropName, tsRangeFieldName)
    val newQuantum = options.get(ReadConf.tsQuantumPropName).map(JavaUtils.timeStringAsMs)
    ReadConf(newFetchSize, newSplitCount, newTsTimestampBinding, newTsRangeFieldName, newQuantum, newUseStreamingValuesForFBRead)
  }
}

object ReadConf {

  final val splitCountPropName = "spark.riak.input.split.count"
  final val useStreamingValuesPropName = "spark.riak.fullbucket.use-streaming-values"
  final val fetchSizePropName = "spark.riak.input.fetch-size"
  final val tsBindingsTimestamp = "spark.riakts.bindings.timestamp"
  final val tsRangeFieldPropName = "spark.riak.partitioning.ts-range-field-name"
  final val tsQuantumPropName = "spark.riak.partitioning.ts-quantum"

  private val defaultProperties: Properties =
     getClass.getResourceAsStream("/ee-default.properties") match {
       case s: InputStream =>
         val p = new Properties()
         p.load(s)
         s.close()
         p
       case _ =>
         new Properties()
     }

  final val DefaultTsTimestampBinding = UseTimestamp

  final val DefaultFetchSize = 1000

  // TODO: Need to think about the proper default value
  final val DefaultSplitCount = 10

  final val DefaultUseStreamingValues4FBRead: Boolean =
    defaultProperties.getProperty(useStreamingValuesPropName, "false")
    .toBoolean

  /** Creates ReadConf based on properties provided to Spark Conf
  *
  * @param conf SparkConf of Spark context with Riak-related properties
  */
  def apply(conf: SparkConf): ReadConf = {
    ReadConf(
      fetchSize = conf.getInt(fetchSizePropName, DefaultFetchSize),
      splitCount = conf.getInt(splitCountPropName, DefaultSplitCount),
      tsTimestampBinding = TsTimestampBindingType(conf.get(tsBindingsTimestamp, DefaultTsTimestampBinding.value)),
      tsRangeFieldName = conf.get(tsRangeFieldPropName, null),
      quantum = conf.getOption(tsQuantumPropName).map(JavaUtils.timeStringAsMs),
      useStreamingValuesForFBRead = conf.getBoolean(useStreamingValuesPropName, DefaultUseStreamingValues4FBRead)
    )
  }

  /** Creates ReadConf based on an externally provided map of properties
  *   to override those of SparkCon
  *
  * @param conf SparkConf of Spark context to be taken as defaults
  * @param options externally provided map of properties
  */
  def apply(conf: SparkConf, options: Map[String, String]): ReadConf = {
    val readConf = ReadConf(conf)
    readConf.overrideProperties(options)
  }
}
