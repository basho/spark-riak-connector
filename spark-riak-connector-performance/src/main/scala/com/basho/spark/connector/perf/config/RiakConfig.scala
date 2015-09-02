package com.basho.spark.connector.perf.config

/**
 * @author anekhaev
 */
trait RiakConfig extends Config{
  
  lazy val riakHost = config.getString("perf-test.riak.host")
  lazy val riakPort = config.getInt("perf-test.riak.port")
  lazy val riakMinConnections = config.getInt("perf-test.riak.min-connections")
  lazy val riakMaxConnections = config.getInt("perf-test.riak.max-connections")
  
}