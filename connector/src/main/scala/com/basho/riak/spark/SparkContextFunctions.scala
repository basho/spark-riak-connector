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

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory
import com.basho.riak.spark.rdd.{ReadConf, RiakRDD, RiakTSRDD}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  def riakTSTable[T](bucketName: String,
                     readConf: ReadConf = ReadConf(sc.getConf),
                     schema: Option[StructType] = None
                    )(implicit
                      ct: ClassTag[T],
                      connector: RiakConnector = RiakConnector(sc.getConf)
                    ): RiakTSRDD[T] = RiakTSRDD[T](sc, bucketName, readConf = readConf, schema = schema)

  def riakBucket[T](bucketName: String,
                    bucketType: String = "default"
                   )(implicit
                     connector: RiakConnector = RiakConnector(sc.getConf),
                     ct: ClassTag[T],
                     rdmf: ReadDataMapperFactory[T]
                   ): RiakRDD[T] =
    new RiakRDD[T](sc, connector, bucketType, bucketName, readConf = ReadConf(sc.getConf))

  def riakBucket[T](ns: Namespace
                   )(implicit
                     ct: ClassTag[T],
                     rdmf: ReadDataMapperFactory[T]
                   ): RiakRDD[T] = riakBucket[T](ns.getBucketNameAsString, ns.getBucketTypeAsString)
}
