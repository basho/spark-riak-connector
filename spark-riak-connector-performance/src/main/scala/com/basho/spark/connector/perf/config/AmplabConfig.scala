package com.basho.spark.connector.perf.config

/**
 * @author anekhaev
 */
trait AmplabConfig extends Config {

  lazy val amplabBucket = config.getString("perf-test.amplab.s3-bucket")
  lazy val amplabPath = config.getString("perf-test.amplab.s3-path")
  
}