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
package com.basho.riak.spark.util

import com.basho.riak.client.core.query.timeseries.CoverageEntry

trait Dumpable {
  def dump(lineSep: String = "\n"): String = toString
}


object DumpUtils {

  def dump(ce: CoverageEntry, lineSep: String): String = {
    val lb = ce.isLowerBoundInclusive match {
      case true => "["
      case false => "("
    }

    val ub = ce.isUpperBoundInclusive match {
      case true => "]"
      case false => ")"
    }

    s"$lb${ce.getLowerBound},${ce.getUpperBound}$ub@host: ${ce.getHost}:${ce.getPort}" + lineSep +
      s"description: ${ce.getDescription}" + lineSep +
      s"context: " + {ce.getCoverageContext match {
      case null => "null"
      case c => c.map("%02X" format _).mkString
    }}
  }
}