package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.client.core.query.indexes.LongIntIndex
import com.basho.riak.spark._
import com.basho.riak.spark.util.RiakObjectConversionUtil
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import com.basho.spark.connector.perf.riak.RiakClient
import org.apache.spark.SparkContext

object LoadDataToRiakBucketApp extends App with RiakConfig with SparkConfig with AmplabConfig {

  val riakBucket = config.getString("perf-test.riak.bucket")
  val riakNameSpace = new Namespace("default", riakBucket)
  val riakClient = new RiakClient(riakHost, riakPort, riakMinConnections)

  riakClient.resetAndEmptyBucket(riakNameSpace)

  val sc = new SparkContext(sparkConfig)

  val hadoopCfg = sc.hadoopConfiguration
  hadoopCfg.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

  val rdd = sc.textFile(s"s3n://$amplabS3Bucket/$amplabS3Path/*")
    .zipWithIndex()
    .map { case (line, index) =>
    val obj = RiakObjectConversionUtil.to(line)
    obj.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named("creationNo"))
      .add(index)
    obj
  }

  println(s"Loaded ${rdd.count()} entities from S3")

  rdd.saveToRiak(riakNameSpace)

  println(s"Entities saved to Riak Bucket $riakBucket")
}