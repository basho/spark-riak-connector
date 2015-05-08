package com.basho.spark.connector.rdd

import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.writer.{WriteConf, ValueWriterFactory, BucketWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDFunctions[T](rdd: RDD[T]) extends Serializable {

  val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Store data from the `RDD` to the specified Riak bucket.
   */
  def saveToRiak(bucketName: String,
                      bucketType: String = "default",
                      writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                     (implicit connector: RiakConnector = RiakConnector(sparkContext.getConf),
                      vwf: ValueWriterFactory[T]): Unit = {
    val writer = BucketWriter[T](connector, bucketType, bucketName, writeConf )
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  def saveToRiak(ns: Namespace)
                (implicit vwf: ValueWriterFactory[T]): Unit = {
    saveToRiak(ns.getBucketNameAsString, ns.getBucketTypeAsString)
  }

  def saveAsRiakBucket(bucketDef: BucketDef, writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                      (implicit connector: RiakConnector = RiakConnector(sparkContext.getConf),
                       vwf: ValueWriterFactory[T]): Unit = {

    val writer = BucketWriter[T](connector, bucketDef, writeConf )
    rdd.sparkContext.runJob(rdd, writer.write _)
  }
}
