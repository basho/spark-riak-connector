package com.basho.spark.connector.perf.config

/**
 * @author anekhaev
 */
trait AmplabConfig extends Config {

  lazy val amplabS3Bucket = config.getString("perf-test.amplab.s3-bucket")
  lazy val amplabS3Path = config.getString("perf-test.amplab.s3-path")
  
  lazy val amplabLocalPath = config.getString("perf-test.amplab.local-path")
  
}