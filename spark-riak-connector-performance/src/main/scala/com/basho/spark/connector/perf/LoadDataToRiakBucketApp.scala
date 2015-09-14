package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import com.basho.spark.connector.perf.dataset.S3AmplabDataset
import com.basho.spark.connector.perf.riak.RiakClient
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext

object LoadDataToRiakBucketApp extends App with RiakConfig with SparkConfig with AmplabConfig {

  val dataset = new S3AmplabDataset(amplabS3Bucket, amplabS3Path, amplabFilesLimit)

  val riakBucket = config.getString("perf-test.riak.bucket")
  val riakNameSpace = new Namespace("default", riakBucket)
  val riakClient = new RiakClient(riakHost, riakPort, riakMinConnections)
  riakClient.resetAndEmptyBucket(riakNameSpace)

  val sc = new SparkContext(sparkConfig)

  val hadoopCfg = sc.hadoopConfiguration
  hadoopCfg.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

  val s3Rdd = sc.textFile(s"s3n://$amplabS3Bucket/$amplabS3Path/*")
    .map(line => StringUtils.replace(line, "'", "\\'"))

  println(s"Loaded ${s3Rdd.count()} entities from S3")

  s3Rdd.saveToRiak(riakNameSpace)

  println(s"Entities saved to Riak Bucket $riakBucket")
}
