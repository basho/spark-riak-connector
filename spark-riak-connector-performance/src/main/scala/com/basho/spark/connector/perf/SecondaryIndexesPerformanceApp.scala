package com.basho.spark.connector.perf

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.spark.connector.perf.config.{AmplabConfig, RiakConfig, SparkConfig}
import com.basho.spark.connector.perf.dataset.S3AmplabDataset
import com.basho.spark.connector.perf.riak.RiakClient
import org.apache.spark.SparkContext



/**
 * @author anekhaev
 */
object SecondaryIndexesPerformanceApp extends App with RiakConfig with SparkConfig with AmplabConfig {
  
  val dataset = new S3AmplabDataset(amplabS3Bucket, amplabS3Path, amplabFilesLimit)
     
  val riakNameSpace = new Namespace("default", "r2i-perf-test")
  val riakClient = new RiakClient(riakHost, riakPort, riakMinConnections)
   
  riakClient.resetAndLoadDataset(riakNameSpace, dataset)
  
  val sc = new SparkContext(sparkConfig)
  
  val records = sc.riakBucket[String](riakNameSpace)
    .query2iRange("testIndex", 0L, 100L)
    .collect()
    
  println(s"Received ${records.size} records")
  
}