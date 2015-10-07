package com.basho.spark.connector.perf

import com.basho.riak.spark._
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import com.basho.spark.connector.perf.util.ConfigurationDump
import org.apache.spark.SparkContext



/**
 * @author anekhaev
 */
object SecondaryIndexesPerformanceApp extends App with RiakConfig with SparkConfig with AmplabConfig with ConfigurationDump {

  val sc = new SparkContext(sparkConfig)
  dump(sc)

  val from = args(1).toInt
  val to = args(2).toInt

  val records = sc.riakBucket[String](amplabRiakNamespace)
    .query2iRange("creationNo", from, to)

  println(s"Received ${records.count()} records")
  
}