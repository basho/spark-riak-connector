package com.basho.spark.connector.perf.config

/**
 * @author anekhaev
 */
trait RiakConfig {
  
  val riakHost = "localhost"
  val riakPort = 8087
  val riakParallelRequests = 4
  
}