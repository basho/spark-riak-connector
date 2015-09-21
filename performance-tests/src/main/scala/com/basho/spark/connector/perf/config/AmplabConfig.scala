package com.basho.spark.connector.perf.config

import com.basho.riak.client.core.query.Namespace

/**
 * @author anekhaev
 */
trait AmplabConfig extends Config { self: App =>

  lazy val amplabS3Bucket = config.getString("perf-test.amplab.s3-bucket")
  lazy val amplabS3Path = config.getString("perf-test.amplab.s3-path")
  
  lazy val amplabLocalPath = config.getString("perf-test.amplab.local-path")
  
  lazy val amplabRiakNamespace = new Namespace("default", config.getString("perf-test.amplab.target-riak-bucket")) 
  
}