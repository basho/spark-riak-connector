package com.basho.riak.spark.writer

import com.basho.riak.spark._
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.writer.ts.RowDef
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row => SparkRow}

trait WritableToRiak[T] {

  def sparkContext: SparkContext

  def saveToRiak(bucketName: String,
                 bucketType: String = BucketDef.DefaultBucketType,
                 writeConf: WriteConf = WriteConf(sparkContext.getConf)
                )(implicit
                  connector: RiakConnector = RiakConnector(sparkContext.getConf),
                  vwf: WriteDataMapperFactory[T, KeyValue]
                ): Unit

  def saveToRiakTS(bucketName: String,
                   bucketType: String = BucketDef.DefaultBucketType,
                   writeConf: WriteConf = WriteConf(sparkContext.getConf)
                  )(implicit
                    evidence: T <:< SparkRow,
                    connector: RiakConnector = RiakConnector(sparkContext.getConf),
                    vwf: WriteDataMapperFactory[T, RowDef]
                  ): Unit
}
