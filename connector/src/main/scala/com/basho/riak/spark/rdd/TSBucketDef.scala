package com.basho.riak.spark.rdd

import com.basho.riak.client.core.query.timeseries.TableDefinition
import java.util.concurrent.TimeUnit
import org.apache.spark.sql.types.StructType
import com.basho.riak.spark.util.TSConversionUtil
case class TSBucketDef(
    tableDefinition: TableDefinition,
    quantum: Int,
    timeUnit: TimeUnit) {
  require(!tableDefinition.getPartitionKeyColumnDescriptions.isEmpty(), "TableDefinition should have partition keys")

  def apply(schema: StructType, name: String, quantum: Int, timeUnit: TimeUnit): TSBucketDef = {
    val tableDef = TSConversionUtil.asTableDef(name, schema)
    TSBucketDef(tableDef, quantum, timeUnit)
  }
}