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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.network.util.JavaUtils

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

/** RDD read settings
 *
  * @param fetchSize number of keys to fetch in a single round-trip to Riak
  * @param _splitCount desired minimum number of Spark partitions to divide the data into, or [[None]]
 */
case class ReadConf (
  val fetchSize: Int = ReadConf.DefaultFetchSize,
  private val _splitCount: Option[Int] = None,
  val tsTimestampBinding: TsTimestampBindingType = ReadConf.DefaultTsTimestampBinding,
  /**
   * Used only in ranged partitioner to identify quantized field.
   * Usage example:
   *    sqlContext.read
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


  def getOrDefaultSplitCount(defaultSplitCount: Int = ReadConf.DefaultSplitCount): Int =
    _splitCount.getOrElse(defaultSplitCount)

  /**
    *  If there is no explicitly specified [[ReadConf._splitCount]] it calculates the split count
    *  in accordance to the in number of available spark workers.
    *
    * The docs recommend to have your number of partitions set to 3 or 4 times the number of CPUs in your cluster
    * so that the work gets distributed more evenly among the CPUs.
    * Meaning, if you only have 1 partition per core in the cluster you will have to wait for the one longest running
    * task to complete but if you had broken that down further the workload would be more evenly balanced with fast
    * and slow running tasks evening out.
    *
    * @param sc SparkContext
    * @param duration to wait while a Spark runtime settled down and all available executors report about their readiness
    * @return desired number of Spark partitions, explicitly specified or calculated
    * @see [[ReadConf.smartSplitMultiplier]]
    */
  def getOrSmartSplitCount(sc: SparkContext, duration: FiniteDuration = 4 second): Int =
    this._splitCount match {
      case Some(split: Int) => split
      case None =>
        // sleep to give spark runtime a chance to settle down with the executors pool
        Thread.sleep(duration.toMillis)
        val totalNumberOfExecutors = sc.getExecutorMemoryStatus.size

        this.getOrDefaultSplitCount(totalNumberOfExecutors * ReadConf.smartSplitMultiplier)
      }

  def overrideProperties(options: Map[String, String]): ReadConf = {
    val newFetchSize = options.getOrElse(ReadConf.fetchSizePropName, fetchSize.toString).toInt
    val newSplitCount = options.get(ReadConf.splitCountPropName).map(_.toInt).orElse(_splitCount)
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

  /**
    * The docs recommend to have your number of partitions set to 3 or 4 times the number of CPUs in your cluster
    * so that the work gets distributed more evenly among the CPUs.
    * Meaning, if you only have 1 partition per core in the cluster you will have to wait for the one longest running
    * task to complete but if you had broken that down further the workload would be more evenly balanced with fast
    * and slow running tasks evening out.
    *
    * Since there is no enough information about available Spark resources such as real number of cores,
    * 3x multiplier will be used.
    */
  final val smartSplitMultiplier = 3

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
      _splitCount = conf.getOption(splitCountPropName).map(_.toInt),
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
