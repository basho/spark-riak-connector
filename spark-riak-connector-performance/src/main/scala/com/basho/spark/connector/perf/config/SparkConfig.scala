package com.basho.spark.connector.perf.config

import org.apache.spark.SparkConf

/**
 * @author anekhaev
 */
trait SparkConfig extends RiakConfig {

  val sparkConfig = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
    .set("spark.riak.connection.host", riakHost + ":" + riakPort)
    .set("spark.riak.output.wquorum", "1")
    .set("spark.riak.input.fetch-size", "5000")

}