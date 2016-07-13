package com.basho.riak.spark.streaming

import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.writer.{RiakWriter, WritableToRiak, WriteConf, WriteDataMapperFactory}
import com.basho.riak.spark.writer.ts.RowDef
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

class DStreamFunctions[T](dstream: DStream[T]) extends WritableToRiak[T] {
  override def sparkContext: SparkContext = dstream.context.sparkContext

  override def saveToRiak(bucketName: String,
                          bucketType: String = BucketDef.DefaultBucketType,
                          writeConf: WriteConf = WriteConf(sparkContext.getConf)
                         )(implicit
                           connector: RiakConnector,
                           vwf: WriteDataMapperFactory[T, (String, Any)]
                         ): Unit = {
    val writer = RiakWriter[T](connector, bucketType, bucketName, writeConf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, writer.write _))
  }

  override def saveToRiakTS(bucketName: String,
                            bucketType: String = BucketDef.DefaultBucketType,
                            writeConf: WriteConf = WriteConf(sparkContext.getConf)
                           )(implicit evidence: <:<[T, Row],
                             connector: RiakConnector,
                             vwf: WriteDataMapperFactory[T, RowDef]
                           ): Unit = {
    val tsWriter = RiakWriter.tsWriter[T](connector, bucketType, bucketName, writeConf)
    dstream.foreachRDD(rdd => rdd.sparkContext.runJob(rdd, tsWriter.write _))
  }
}
