/**
  * Copyright (c) 2015-2017 Basho Technologies, Inc.
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
package com.basho.riak.spark.util

import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.{CoverageEntry => KVCoverageEntry}
import com.basho.riak.client.core.query.timeseries.{CoverageEntry => TSCoverageEntry}
import com.basho.riak.client.core.util.HostAndPort

import scala.collection.TraversableLike

trait Dumpable {
  def dump(lineSep: String = "\n"): String = toString
}


object DumpUtils {

  def dump(hp: HostAndPort):String =
    hp.getHost + ":" + hp.getPort

  def dump(ce: KVCoverageEntry, lineSep: String): String = {
    s"host: ${ce.getHost}:${ce.getPort}" + lineSep +
      s"description: ${ce.getDescription}" + lineSep +
      "context: " + {ce.getCoverageContext match {
      case null => "null"
      case c => c.map("%02X" format _).mkString
    }}
  }

  def dump(ce: TSCoverageEntry, lineSep: String): String = {
    val lb = ce.isLowerBoundInclusive match {
      case true => "["
      case false => "("
    }

    val ub = ce.isUpperBoundInclusive match {
      case true => "]"
      case false => ")"
    }

    s"host: ${ce.getHost}:${ce.getPort}" + lineSep +
      s"range: $lb${ce.getLowerBound},${ce.getUpperBound}$ub" + lineSep +
      "description: " + ce.getDescription + lineSep +
      "context: " + {ce.getCoverageContext match {
      case null => "null"
      case c => c.map("%02X" format _).mkString
    }}
  }

  private def mkDump(o: Any, lineSep: String): String =
    o match {
      case d: Dumpable =>
        d.dump(lineSep)

      case ce: TSCoverageEntry =>
        DumpUtils.dump(ce, lineSep)

      case ce: KVCoverageEntry=>
        DumpUtils.dump(ce, lineSep)

      case h: HostAndPort =>
        DumpUtils.dump(h)
    }

  def dumpWithIdx[T, Repr](traversable: TraversableLike[T, Repr], lineSep: String): String =
    traversable.foldLeft("", 0) {
      (t :(String, Int), r) =>
        (
          t._1 +
            (
              t._1 match {
                case "" => ""
                case  _ => lineSep
              }
              ) +
            s"[${t._2}] " + mkDump(r, lineSep),
          t._2 +1
        )
    }._1

  def dump[T, Repr](traversable: TraversableLike[T, Repr], lineSep: String): String =
    traversable.foldLeft("") {
      (sb, r) =>
        (sb match {
          case "" =>
            ""

          case _ =>
            sb + lineSep
        }) + mkDump(r, lineSep)
    }
}