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

import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.spark.util.{DumpUtils, Dumpable}

case class QueryData[T] (
  // Might be None if represents the full bucket read
  keysOrRange: Option[Either[Seq[T],Seq[(T, Option[T])]]],

  // Might be None if represents the bucket keys
  index: Option[String] = None,

  coverageEntries: Option[Seq[CoverageEntry]] = None
) extends Dumpable {

  def copy( keysOrRange: Option[Either[Seq[T],Seq[(T, Option[T])]]] = keysOrRange,
            index: Option[String] = index,
            coverageEntries: Option[Seq[CoverageEntry]] = coverageEntries ): QueryData[T] ={
    new QueryData[T]( keysOrRange, index, coverageEntries)
  }

  override def dump(lineSep: String): String = {
    s"index: '${index.getOrElse("")}'${lineSep}" +
      s"coverageEntries:" +
      s"${coverageEntries match {
        case Some(entries: Seq[CoverageEntry]) => s"$lineSep  " + DumpUtils.dumpWithIdx(entries, lineSep + "    ")
        case None => "[]"
      }}"
  }
}

object QueryData {
  def create2iKeyRanges[K](index: String, ranges: (K, Option[K])*):QueryData[K] ={
    new QueryData[K](Some(Right(ranges)), Some(index))
  }

  def create2iKeys[K](index: String, keys: K*):QueryData[K] ={
    new QueryData[K](Some(Left(keys)), Some(index))
  }

  def createBucketKeys(keys: String*):QueryData[String] ={
    new QueryData[String](Some(Left(keys)))
  }

  def createReadLocal(ce: CoverageEntry*): QueryData[CoverageEntry] ={
    new QueryData[CoverageEntry](keysOrRange=None, index=Some("$bucket"), coverageEntries = Some(ce))
  }

  def create2iKeyRangesLocal[K](index: String, ranges: (K, Option[K])*): QueryData[K] ={
    new QueryData[K](keysOrRange=Some(Right(ranges)), index=Some(index), coverageEntries = Some(Array.empty[CoverageEntry]))
  }
}
