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
import com.basho.spark.connector.perf.dataset.FileAmplabDataset
import com.basho.spark.connector.perf.config.AmplabConfig



/**
 * @author anekhaev
 */
object FullBucketReadPerformanceApp extends App with RiakConfig with SparkConfig with AmplabConfig {
  
  val dataset = new FileAmplabDataset(amplabLocalPath, 10)
     
  val riakNameSpace = new Namespace("default", "fbr-perf-test")
  val riakClient = new RiakClient(riakHost, riakPort, riakMinConnections)
   
  riakClient.resetAndLoadDataset(riakNameSpace, dataset)
  
  val sc = new SparkContext(sparkConfig)
  
  val records = sc.riakBucket[String](riakNameSpace)
    .queryAll()
    .collect()
    
  println(s"Received ${records.size} records")
  
}