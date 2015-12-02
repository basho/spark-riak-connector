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
package com.basho.riak.spark

import java.io.Serializable

import com.basho.riak.client.core.query.{Namespace, Location, RiakObject}
import com.basho.riak.spark.rdd.{RiakTSRDD, RiakRDD, RiakConnector, ReadConf}
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.SparkContext

import scala.reflect.ClassTag
import scala.runtime.AbstractFunction2

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  protected class PairConversionFunction[V](implicit ctV :ClassTag[V])
      extends AbstractFunction2[Location, RiakObject, (String,V)] with Serializable {

    def apply(l: Location, r: RiakObject): (String,V) = {
      val k = l.getKeyAsString
      k -> RiakObjectConversionUtil.from(l, r)
    }
  }

  def riakTSBucket[T](bucketName: String)
                   (implicit ct: ClassTag[T]): RiakTSRDD[T] =
    RiakTSRDD[T](sc, bucketName)

  def riakBucket[T](bucketName: String)
                   (implicit ct: ClassTag[T]): RiakRDD[T] =
    riakBucket[T](bucketName, "default", RiakObjectConversionUtil.from[T] _)

  def riakBucket[T](bucketName: String, bucketType: String, convert: (Location, RiakObject) => T)
                   (implicit connector: RiakConnector = RiakConnector(sc.getConf),
                    ct: ClassTag[T]): RiakRDD[T] =
      new RiakRDD[T](sc, connector, bucketType, bucketName, convert, readConf = ReadConf.fromSparkConf(sc.getConf))

  def riakBucket[T](ns: Namespace)
                   (implicit ct: ClassTag[T]): RiakRDD[T]  =
    riakBucket[T](ns.getBucketNameAsString, ns.getBucketTypeAsString, RiakObjectConversionUtil.from[T] _)

  def riakBucket[K, V](bucketName: String, convert: (Location, RiakObject) => (K, V))
                      (implicit ct: ClassTag[(K, V)], ctV: ClassTag[V]):RiakRDD[(K,V)] =
    riakBucket[K,V](bucketName, "default", convert)

  /*
   * Creates RiakRDD containing results as Tuple2 objects of <Key> and <Value> queried from Riak bucket.
   * Convert function should be provided to specify how exactly <Key> and <Value> should be retrieved from RiakObject and populated to Tuple2.
   * Example:
   *
   * sc.riakBucket[String, String]("Bucket", (k: Location, r: RiakObject) => (k.getKeyAsString, (RiakObjectConversionUtil.from[String](r))))
     .query2iRange(CREATION_INDEX, 1, 2)
   *
   * @param bucketName name of the Riak bucket
   * @param convert function to perform convertion from Riak <Location> and <RiakObject> to Tuple2[K, V]
   * @return RiakRDD of tuples
   */
  def riakBucket[K, V](bucketName: String, bucketType: String, convert: (Location, RiakObject) => (K, V))
                        (implicit ct: ClassTag[(K, V)], ctV: ClassTag[V]): RiakRDD[(K,V)] =
    riakBucket[(K,V)](bucketName, bucketType, convert)

  def riakBucket[V](bucketName: String, bucketType: String)
                      (implicit ct: ClassTag[(String, V)], ctV: ClassTag[V]): RiakRDD[(String,V)] =
    riakBucket[(String,V)](bucketName, bucketType, new PairConversionFunction[V]())

  def riakBucket[K, V](ns: Namespace, convert: (Location, RiakObject) => (K, V))
                      (implicit ct: ClassTag[(K, V)], ctV: ClassTag[V]): RiakRDD[(K,V)] =
    riakBucket[K,V](ns.getBucketNameAsString, ns.getBucketTypeAsString, convert)
}
