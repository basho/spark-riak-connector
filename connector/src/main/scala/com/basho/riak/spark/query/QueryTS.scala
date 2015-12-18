/**
  * Copyright (c) 2015 Basho Technologies, Inc.
  *
  * This file is provided to you under the Apache License,
  * Version 2.0 (the "License"); you may not use this file
  * except in compliance with the License.  You may obtain
  * a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */
package com.basho.riak.spark.query

import java.sql.Timestamp

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.timeseries.{Row, ColumnDescription}
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}
import org.apache.spark.sql.types.UTF8String
import scala.collection.convert.decorateAsScala._

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
case class TSQueryData(sql: String, values: Seq[Any])

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  */
case class QueryTS(bucket: BucketDef, queryData: TSQueryData, readConf: ReadConf) {

  // TODO: Remove as interpolation was implemented as part of the Riak TS query
  def interpolateValues(sql: String, values: Seq[Any]): String = {
    val regex = "\\?".r

    def recursiveInterpolateFirst(input: String, iterator: Iterator[Any]): String = iterator.isEmpty match {
      case true =>
        input

      case _ =>
        val rv = iterator.next()

        val v = rv match {
          case ts: Timestamp =>
            ts.getTime.toString

          case s: UTF8String =>
            "'" + s + "'"

          case x =>
            x.toString
        }
        recursiveInterpolateFirst(regex.replaceFirstIn(input, v), iterator)
    }
    recursiveInterpolateFirst(sql, values.iterator)
  }

  def nextChunk(session: RiakClient): (Seq[ColumnDescription], Seq[Row]) = {
    val sql = interpolateValues(queryData.sql, queryData.values)
    val request = new com.basho.riak.client.api.commands.timeseries.Query.Builder(sql).build()
    val response = session.execute(request)
    response.getColumnDescriptions.asScala -> response.getRows.asScala
  }
}
