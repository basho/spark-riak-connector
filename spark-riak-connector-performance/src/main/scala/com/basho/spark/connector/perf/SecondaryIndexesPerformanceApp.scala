package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.perf.riak.RiakClient
import com.basho.spark.connector.perf.dataset.AmplabDataset
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.basho.spark.connector._
import com.basho.riak.client.core.util.HostAndPort
import com.basho.spark.connector.perf.config.RiakConfig
import com.basho.spark.connector.perf.config.SparkConfig



/**
 * @author anekhaev
 */
object SecondaryIndexesPerformanceApp extends App with RiakConfig with SparkConfig {
  
  val dataset = new AmplabDataset()
     
  val riakNameSpace = new Namespace("default", "r2i-perf-test")
  val riakClient = new RiakClient(riakHost, riakPort, riakParallelRequests)
   
  riakClient.resetAndLoadDataset(riakNameSpace, dataset)
  
  val sc = new SparkContext(sparkConfig)
  
  val records = sc.riakBucket[String](riakNameSpace)
    .query2iRange("testIndex", 0L, 100L)
    .collect()
    
  println(s"Received ${records.size} records")
  
}