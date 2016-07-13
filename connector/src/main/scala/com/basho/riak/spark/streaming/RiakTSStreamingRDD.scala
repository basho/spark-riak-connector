package com.basho.riak.spark.streaming

import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.{ReadConf, RiakTSRDD}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag


class RiakTSStreamingRDD[R] private[spark](
    ssc: StreamingContext,
    connector: RiakConnector,
    bucketName: String,
    schema: Option[StructType] = None,
    columnNames: Option[Seq[String]] = None,
    whereConstraints: Option[(String, Seq[Any])] = None,
    filters: Array[Filter] = Array(),
    tsRangeFieldName: Option[String] = None,
    quantum: Option[Long] = None,
    query: Option[String] = None,
    readConf: ReadConf = ReadConf())(
  implicit
    ct: ClassTag[R])
  extends RiakTSRDD[R](
    sc = ssc.sparkContext,
    connector = connector,
    bucketName = bucketName,
    schema = schema,
    columnNames = columnNames,
    whereConstraints = whereConstraints,
    filters = filters,
    tsRangeFieldName = tsRangeFieldName,
    quantum = quantum,
    query = query,
    readConf = readConf)
