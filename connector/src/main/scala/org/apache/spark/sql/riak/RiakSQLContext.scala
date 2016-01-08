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
package org.apache.spark.sql.riak

import com.basho.riak.spark.rdd.RiakConnector
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Allows to execute SQL queries against Riak TS.
  *
  * @author Sergey Galkin <srggal at gmail dot com>
  */
class RiakSQLContext(sc: SparkContext, val bucket: String) extends SQLContext(sc) {

  /** A catalyst metadata catalog that points to Riak. */
  @transient
  override protected[sql] lazy val catalog = new RiakCatalog(this, RiakConnector(sc.getConf))

  /** Executes SQL query against Riak TS and returns DataFrame representing the result. */
  def riakTsSql(tsQuery: String): DataFrame = new DataFrame(this, super.parseSql(tsQuery))

  /** Delegates to [[riakTsSql]] */
  override def sql(tsQuery: String): DataFrame = riakTsSql(tsQuery)

}