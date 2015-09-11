package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector._
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import org.apache.spark.SparkContext



/**
 * @author anekhaev
 */
object FullBucketReadPerformanceApp extends App with RiakConfig with SparkConfig with AmplabConfig {
  
  val riakNameSpace = new Namespace("default", config.getString("perf-test.riak.bucket"))

  val sc = new SparkContext(sparkConfig)
  
  val records = sc.riakBucket[String](riakNameSpace)
    .queryAll()
  records.persist()

  println(s"Received ${records.count()} records")
  
}