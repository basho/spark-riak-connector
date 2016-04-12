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
package com.basho.riak.spark.query

import com.basho.riak.client.core.query.Location
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}

import scala.collection.mutable.ArrayBuffer

private case class QueryBucketKeys(bucket: BucketDef,
                                   readConf:ReadConf,
                                   riakConnector: RiakConnector,
                                   keys: Iterable[String]
                                  ) extends QuerySubsetOfKeys[String] {

  override def locationsByKeys(keys: Iterator[String]): (Boolean, Iterable[Location]) = {

    val dataBuffer = new ArrayBuffer[Location](readConf.fetchSize)

    val ns = bucket.asNamespace()

    keys.forall(k =>{
      dataBuffer += new Location(ns, k)
      dataBuffer.size < readConf.fetchSize} )
    false -> dataBuffer
  }
}
