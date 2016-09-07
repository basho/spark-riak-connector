/**
  * Copyright (c) 2015-2016 Basho Technologies, Inc.
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

import com.basho.riak.client.core.query.timeseries.Row
import org.apache.spark.Logging
import com.basho.riak.client.core.query.timeseries.ColumnDescription

class TSDataQueryingIterator(query: QueryTS) extends Iterator[Row] with Logging {

  private var _iterator: Option[Iterator[Row]] = None
  private val subqueries = query.queryData.iterator
  private var columns: Option[Seq[ColumnDescription]] = None

  /**
    * @return result always corresponds to the latest data returned by next() if there were no subsequent call [[hasNext]]
    *         or there is no outstanding data except following cases:
    *
    * - when [[next()]] and [[hasNext]] methods haven't been called yet, then data will be fetched implicitly
    * and corresponding ColumnDefs will be returned.
    * - when [[hasNext]] has been called and there is a next [[Row]], columnDefs for the next [[Row]] will be returned then,
    */
  def columnDefs: Seq[ColumnDescription] = {
    if (_iterator.isEmpty && columns.isEmpty) {
      /**
        *  if it is a newly created data iterator it has no outstanding data from the previous fetch(prefetch)
        *  and it is safe to do a prefetch and use column defs returned by it
       */
      prefetch()
    }

    columns match {
      case None      => Seq()
      case Some(cds) => cds
    }
  }
  
  protected[this] def prefetch() = {
    while( subqueries.hasNext && !isPrefetchedDataAvailable) {
      val nextSubQuery = subqueries.next
      logTrace(s"Prefetching chunk of data: ts-query(token=$nextSubQuery)")

      val r = query.nextChunk(nextSubQuery)

      r match {
        case (cds, rows) =>
          if (isTraceEnabled()) {
            logTrace(s"ts-query($nextSubQuery) returns:\n  columns: ${r._1}\n  data:\n\t ${r._2}")
          } else {
            logDebug(s"ts-query($nextSubQuery) returns:\n  data.size: ${r._2.size}")
          }

          if (cds != null && cds.nonEmpty) {
            columns = Some(cds)
          } else if (columns.isEmpty) {
            // We have to initialize columns here, to make a difference and use it as indikator
            columns = Some(Seq())
          }

          _iterator = Some(rows.iterator)

        case _ => _iterator = None
          logWarning(s"ts-query(token=$nextSubQuery) returns: NOTHING")
      }
    }
  }

  private def isPrefetchedDataAvailable: Boolean =
    !(_iterator.isEmpty || (_iterator.isDefined && !_iterator.get.hasNext))

  override def hasNext: Boolean = {
    if (!isPrefetchedDataAvailable) {
        prefetch()
    }

    _iterator match {
      case Some(it) => it.hasNext
      case None     => false
    }
  }

  override def next(): Row = {
    if (!hasNext) {
      throw new NoSuchElementException("next on empty iterator")
    }
    _iterator.get.next
  }
}

object TSDataQueryingIterator {

  def apply[R](query: QueryTS): TSDataQueryingIterator = new TSDataQueryingIterator(query)
}
