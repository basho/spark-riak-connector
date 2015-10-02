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

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.operations.ts.QueryOperation
import com.basho.riak.client.core.query.timeseries.{Row, ColumnDescription}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}
import scala.collection.convert.decorateAsScala._

case class QueryTS[String](bucket: BucketDef, query: String, readConf: ReadConf) {
  def nextChunk(session: RiakClient): (Seq[ColumnDescription], Seq[Row]) = {
    val op = new QueryOperation.Builder(BinaryValue.create(query.toString)).build()
    val qr = session.getRiakCluster.execute(op).get()
    qr.getColumnDescriptions.asScala -> qr.getRows.asScala
  }
}
