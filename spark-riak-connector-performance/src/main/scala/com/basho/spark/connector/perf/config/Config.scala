package com.basho.spark.connector.perf.config

import com.typesafe.config.ConfigFactory

/**
 * @author anekhaev
 */
trait Config { 
  
  lazy val config = ConfigFactory.load()
  
}