package com.basho.riak.spark.streaming

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark.SparkContextFunctions
import com.basho.riak.spark.rdd.{ReadConf, RiakRDD, RiakTSRDD}
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

class StreamingContextFunctions(ssc: StreamingContext) extends SparkContextFunctions(ssc.sparkContext) {
  override def riakTSTable[T](bucketName: String,
                              readConf: ReadConf,
                              schema: Option[StructType]
                             )(implicit
                               ct: ClassTag[T],
                               connector: RiakConnector
                             ): RiakTSRDD[T] = new RiakTSStreamingRDD[T](ssc, connector, bucketName, schema)

  override def riakBucket[T](bucketName: String,
                             bucketType: String
                            )(implicit
                              connector: RiakConnector,
                              ct: ClassTag[T],
                              rdmf: ReadDataMapperFactory[T]
                            ): RiakRDD[T] =
    new RiakStreamingRDD[T](ssc, connector, bucketType, bucketName, readConf = ReadConf(ssc.sparkContext.getConf))

  override def riakBucket[T](ns: Namespace
                            )(implicit
                              ct: ClassTag[T],
                              rdmf: ReadDataMapperFactory[T]
                            ): RiakRDD[T] = riakBucket(ns.getBucketNameAsString, ns.getBucketTypeAsString)
}
