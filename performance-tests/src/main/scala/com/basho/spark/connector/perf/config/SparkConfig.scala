package com.basho.spark.connector.perf.config

import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/**
 * @author anekhaev
 */
trait SparkConfig extends Config { self: App =>

  lazy val sparkConfig = {
    val sparkConfigSelection = config.getConfig("perf-test.spark")
    val sparkKeySection = sparkConfigSelection.entrySet().toList.map("spark." + _.getKey)
    val configSection = config.getConfig("perf-test")

    sparkKeySection.foldLeft(new SparkConf(true).setAppName(getClass.getSimpleName)) { (cfg, key) => 
      cfg.set(key, configSection.getString(key))
    }
  }
   

}