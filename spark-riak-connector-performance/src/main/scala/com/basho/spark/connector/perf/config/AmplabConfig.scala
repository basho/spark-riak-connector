package com.basho.spark.connector.perf.config

/**
 * @author anekhaev
 */
trait AmplabConfig extends Config { self: App =>

  lazy val amplabS3Bucket = config.getString("perf-test.amplab.s3-bucket")
  lazy val amplabS3Path = config.getString("perf-test.amplab.s3-path")
  
  lazy val amplabLocalPath = config.getString("perf-test.amplab.local-path")
  
  lazy val amplabFilesLimit = 
    if (config.getBoolean("perf-test.amplab.limit-dataset"))
      Some(config.getInt("perf-test.amplab.load-files-limit"))
    else
      None
  
}