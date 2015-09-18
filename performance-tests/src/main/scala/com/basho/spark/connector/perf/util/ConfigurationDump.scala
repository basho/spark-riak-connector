package com.basho.spark.connector.perf.util

import com.basho.spark.connector.perf.config.Config
import org.apache.spark.SparkContext

trait ConfigurationDump { self: Config =>

  def dump(sc: SparkContext): Unit = {
    println("------ [Job configuration]")
    println(s"           spark.app.id = ${sc.applicationId}")
    println(s"           spark.riak.input.fetch-size = ${sc.getConf.get("spark.riak.input.fetch-size")}")
    println(s"           data-size = ${config.getString("perf-test.amplab.data-size")}")
    println("------ [End of job configuration]")
  }

}
