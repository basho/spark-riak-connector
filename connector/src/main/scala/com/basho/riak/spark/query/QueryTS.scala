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

import java.util.concurrent.ExecutionException

import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.operations.ts.QueryOperation
import com.basho.riak.client.core.query.timeseries.{ColumnDescription, Row}

import scala.collection.convert.decorateAsScala._
import com.basho.riak.client.core.query.timeseries.CoverageEntry
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.util.{Dumpable, DumpUtils}

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.1.0
  */
case class TSQueryData(sql: String, coverageEntry: Option[CoverageEntry] = None) extends Dumpable {
    val primaryHost = coverageEntry.map(e => HostAndPort.fromParts(e.getHost, e.getPort))

  override def dump(lineSep: String = "\n"): String = {
    val optional = coverageEntry match {
      case Some(ce) => lineSep + s"primary-host: ${primaryHost.get.getHost}:${primaryHost.get.getPort}" + lineSep +
        "coverage-entry:" +lineSep + "   " +
          DumpUtils.dump(ce, lineSep + "   ")
      case None => ""
    }

    s"sql: {${sql.toLowerCase.replaceAll("\n", "")}}" + optional
  }
}

/**
 * @author Sergey Galkin <srggal at gmail dot com>
 * @since 1.1.0
 */
case class QueryTS(connector: RiakConnector, queryData: Seq[TSQueryData]) {


  def nextChunk(tsQueryData: TSQueryData): (Seq[ColumnDescription], Seq[Row]) = {
    val op = tsQueryData.coverageEntry match {
      case None     => new QueryOperation.Builder(tsQueryData.sql).build()
      case Some(ce) => new QueryOperation.Builder(tsQueryData.sql).withCoverageContext(ce.getCoverageContext()).build()
    }

    try {
      connector.withSessionDo(tsQueryData.primaryHost.map(Seq(_)))({ session =>
        val qr = session.execute(op).get()
        qr.getColumnDescriptionsCopy.asScala -> qr.getRowsCopy.asScala
      })
    } catch {
      case e: ExecutionException =>
        if (e.getCause.isInstanceOf[RiakResponseException]
          && e.getCause.getMessage.equals("Unknown message code: 90")) {
          throw new IllegalStateException("Range queries are not supported in your version of Riak", e.getCause)
        } else {
          throw e
        }
    }
  }
}