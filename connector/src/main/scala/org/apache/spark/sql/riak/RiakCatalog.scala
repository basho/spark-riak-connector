/*******************************************************************************
 * Copyright (c) 2016 IBM Corp.
 * 
 * Created by Basho Technologies for IBM
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *******************************************************************************/
package org.apache.spark.sql.riak

import java.util.concurrent.ExecutionException

import com.basho.riak.client.core.netty.RiakResponseException
import com.basho.riak.client.core.operations.FetchBucketPropsOperation
import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.rdd.ReadConf
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.writer.WriteConf
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.{TableIdentifier, SimpleCatalystConf}
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
  * @author Sergey Galkin <srggal at gmail dot com>
  * @since 1.2.0
  */
private[sql] class RiakCatalog(rsc: RiakSQLContext,
                               riakConnector: RiakConnector,
                               readConf: ReadConf,
                               writeConf: WriteConf) extends Catalog with Logging {
  private val CACHE_SIZE = 1000

  /** A cache of Spark SQL data source tables that have been accessed. Cache is thread safe. */
  private[riak] val cachedDataSourceTables: LoadingCache[String, LogicalPlan] = {
    val cacheLoader = new CacheLoader[String, LogicalPlan]() {
      override def load(tableIdent: String): LogicalPlan = {
        logDebug(s"Creating new cached data source for ${tableIdent.mkString(".")}")
        buildRelation(tableIdent)
      }
    }
    CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build(cacheLoader)
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    val table = bucketIdent(tableIdent)
    cachedDataSourceTables.refresh(table)
  }

  override val conf: SimpleCatalystConf = SimpleCatalystConf(true)


  override def unregisterAllTables(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }

  override def unregisterTable(tableIdentifier: TableIdentifier): Unit = {
    val tableIdent = bucketIdent(tableIdentifier)
    cachedDataSourceTables.invalidate(tableIdent)
  }

  override def lookupRelation(tableIdentifier: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val tableIdent = bucketIdent(tableIdentifier)
    val tableLogicPlan = cachedDataSourceTables.get(tableIdent)
    alias.map(a => Subquery(a, tableLogicPlan)).getOrElse(tableLogicPlan)
  }

  override def registerTable(tableIdentifier: TableIdentifier, plan: LogicalPlan): Unit = {
    val tableIdent = bucketIdent(tableIdentifier)
    cachedDataSourceTables.put(tableIdent, plan)
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    getTablesFromRiakTS(databaseName)
  }

  override def tableExists(tableIdentifier: TableIdentifier): Boolean = {
    val tableIdent = bucketIdent(tableIdentifier)
    val fetchProps = new FetchBucketPropsOperation.Builder(new Namespace(tableIdent, tableIdent)).build()

    riakConnector.withSessionDo(session => {
      session.execute(fetchProps)
    })

    try {
      fetchProps.get().getBucketProperties
      true
    } catch {
      case ex: ExecutionException if ex.getCause.isInstanceOf[RiakResponseException]
        && ex.getCause.getMessage.startsWith("No bucket-type named") =>
        false
    }
  }

  def getTablesFromRiakTS(databaseName: Option[String]): Seq[(String, Boolean)] = Nil

  /** Build logic plan from a RiakRelation */
  private def buildRelation(tableIdent: String): LogicalPlan = {
    val relation = RiakRelation(tableIdent, rsc, None, Some(riakConnector), readConf, writeConf)
    Subquery(tableIdent, LogicalRelation(relation))
  }

  /** Return a table identifier with table name, keyspace name and cluster name */
  private def bucketIdent(tableIdentifier: Seq[String]): String = {
    require(tableIdentifier.size == 1)
    tableIdentifier.head
  }

  private def bucketIdent(tableIdentifier: TableIdentifier): String = {
    tableIdentifier.table
  }
}
