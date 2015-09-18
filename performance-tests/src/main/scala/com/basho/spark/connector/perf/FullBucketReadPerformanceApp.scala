package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import com.basho.spark.connector.perf.util.ConfigurationDump
import org.apache.spark.SparkContext



/**
 * @author anekhaev
 */
object FullBucketReadPerformanceApp extends App with RiakConfig with SparkConfig with AmplabConfig with ConfigurationDump {
  
  val riakNameSpace = new Namespace("default", config.getString("perf-test.riak.bucket"))

  val sc = new SparkContext(sparkConfig)
  dump(sc)

  val records = sc.riakBucket[String](riakNameSpace)
    .queryAll()

  println(s"Received ${records.count()} records")
  
}