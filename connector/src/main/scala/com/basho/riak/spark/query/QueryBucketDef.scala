package com.basho.riak.spark.query

import scala.collection.JavaConversions._
import scala.util.Try

import org.apache.spark.sql.types.StructType

import com.basho.riak.client.core.operations.ts.DescribeTableOperation
import com.basho.riak.client.core.query.timeseries.TableDefinition
import com.basho.riak.spark.rdd.ReadConf
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.util.TSConversionUtil._

case class QueryBucketDef(connector: RiakConnector, readConf: ReadConf = ReadConf()) {

  def getTableDef(name: String): Option[TableDefinition] = {
    val describeOp = new DescribeTableOperation.Builder(name).build()
    connector.withSessionDo(session => {
      session.execute(describeOp)
    })
    Try(describeOp.get()).toOption
  }

  def getTableSchema(name: String): Option[StructType] = {
    getTableDef(name).map(x => asSparkSchema(x.getFullColumnDescriptions.toSeq, readConf.tsTimestampBinding))
  }
}