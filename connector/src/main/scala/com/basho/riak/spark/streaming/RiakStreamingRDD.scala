package com.basho.riak.spark.streaming

import com.basho.riak.spark.query.QueryData
import com.basho.riak.spark.rdd.{ReadConf, RiakRDD}
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

class RiakStreamingRDD[R] private[spark](
    ssc: StreamingContext,
    connector: RiakConnector,
    bucketType: String,
    bucketName: String,
    queryData: Option[QueryData[_]] = None,
    readConf: ReadConf = ReadConf())(
  implicit
    ct: ClassTag[R],
    @transient rdmf: ReadDataMapperFactory[R])
  extends RiakRDD[R](
    sc = ssc.sparkContext,
    connector = connector,
    bucketType = bucketType,
    bucketName = bucketName,
    queryData = queryData,
    readConf = readConf)
