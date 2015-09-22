package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.indexes.LongIntIndex
import com.basho.riak.spark._
import com.basho.riak.spark.util.RiakObjectConversionUtil
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import com.basho.spark.connector.perf.dataset.S3Client
import com.basho.spark.connector.perf.riak.RiakClient
import com.basho.spark.connector.perf.util.ConfigurationDump
import org.apache.spark.{Logging, SparkContext}

object LoadDataToRiakBucketApp extends App with RiakConfig with SparkConfig with AmplabConfig with ConfigurationDump with Logging {

  val riakClient = new RiakClient(riakHost, riakPort, riakMinConnections)

  riakClient.resetAndEmptyBucket(amplabRiakNamespace)

  val sc = new SparkContext(sparkConfig)

  val awsCredentials = S3Client.defaultS3Credentials.getCredentials
  val hadoopCfg = sc.hadoopConfiguration
  hadoopCfg.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  hadoopCfg.set("fs.s3n.awsAccessKeyId", awsCredentials.getAWSAccessKeyId)
  hadoopCfg.set("fs.s3n.awsSecretAccessKey", awsCredentials.getAWSSecretKey)

  dump(sc)

  val allS3Files = S3Client.listChildrenKeys(amplabS3Bucket, amplabS3Path).map(key => s"s3n://$amplabS3Bucket/$key")
  logInfo(s"There are ${allS3Files.size} files on S3")

  val filesToLoad = if (filesLimit == 0) allS3Files else allS3Files.take(filesLimit)
  logInfo(s"${filesToLoad.size} files will be loaded")

  val rdd = sc.union(filesToLoad.map(sc.textFile(_)))
    .zipWithIndex()
    .map { case (line, index) =>
      val obj = RiakObjectConversionUtil.to(line)
      obj.getIndexes.getIndex[LongIntIndex, LongIntIndex.Name](LongIntIndex.named("creationNo"))
        .add(index)
      obj
    }

  logInfo(s"Loaded ${rdd.count()} entities from S3")

  rdd.saveToRiak(amplabRiakNamespace)

  logInfo(s"Entities saved to Riak Bucket $amplabRiakNamespace")
}