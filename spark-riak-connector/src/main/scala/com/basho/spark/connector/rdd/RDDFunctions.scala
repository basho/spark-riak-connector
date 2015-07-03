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
package com.basho.spark.connector.rdd

import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.writer.{WriteConf, WriteDataMapperFactory, BucketWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDFunctions[T](rdd: RDD[T]) extends Serializable {

  val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Store data from the `RDD` to the specified Riak bucket.
   */
  def saveToRiak(bucketName: String,
                      bucketType: String = "default",
                      writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                     (implicit connector: RiakConnector = RiakConnector(sparkContext.getConf),
                      vwf: WriteDataMapperFactory[T]): Unit = {
    val writer = BucketWriter[T](connector, bucketType, bucketName, writeConf )
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  def saveToRiak(ns: Namespace)
                (implicit vwf: WriteDataMapperFactory[T]): Unit = {
    saveToRiak(ns.getBucketNameAsString, ns.getBucketTypeAsString)
  }

  def saveAsRiakBucket(bucketDef: BucketDef, writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                      (implicit connector: RiakConnector = RiakConnector(sparkContext.getConf),
                       vwf: WriteDataMapperFactory[T]): Unit = {

    val writer = BucketWriter[T](connector, bucketDef, writeConf )
    rdd.sparkContext.runJob(rdd, writer.write _)
  }
}
