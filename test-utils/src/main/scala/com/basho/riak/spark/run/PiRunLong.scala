package com.basho.riak.spark.run

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._

object LongJobApp {
  val USAGE = "Usage: LongJobApp.jar [--iter num] [--durtaion sec] [--master spark://host{ip}:port] [--samples] [--partitions"
  val NUM_SAMPLES = 2048
  val PARTITIONS = 3
  val APP_NAME = "Pi Darts"
  val SPARK_URL = "spark://node1.local:7077"

  def calc_pi(count: RDD[Int]) = {
    val x = count.map(x => {
      val x = Math.random()
      val y = Math.random()
      if (x*x + y*y < 1) 1 else 0
    }).reduce(_ + _)
    println("Pi is roughly " + 4.0 * x / NUM_SAMPLES)
  }

  def nextOption(map: Map[Symbol, Any], list: List[String]) : Map[Symbol, Any] = {
    list match {
      case Nil => map
      case "--iter" :: value :: tail => nextOption(map ++ Map('iter -> value.toInt), tail)
      case "--duration" :: value :: tail => nextOption(map ++ Map('duration -> value.toInt), tail)
      case "--master" :: value :: tail => nextOption(map ++ Map('master -> value), tail)
      case "--samples" :: value :: tail => nextOption(map ++ Map('samples -> value.toInt), tail)
      case "--partitions" :: value :: tail => nextOption(map ++ Map('partitions -> value.toInt), tail)
      case option :: tail => println("Unknown option " + option)
        println(USAGE)
        sys.exit(1)
    }
  }

  def main(args: Array[String]) = {
    val argList = args.toList
    val options = nextOption(Map(),argList)

    val conf = new SparkConf().setAppName(APP_NAME)
      .setMaster(options.getOrElse('master, SPARK_URL).asInstanceOf[String])
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext

    val count = sc.parallelize(1 to options.getOrElse('samples, NUM_SAMPLES).asInstanceOf[Int],
      options.getOrElse('partitions, PARTITIONS).asInstanceOf[Int])


    if (options.contains('iter)) {
      val n = options.getOrElse('iter, 100).asInstanceOf[Int]
      for (i <- 1 to n) {
        println("======================================")
        println("Iteration " + i + " of " + n)
        println("======================================")
        calc_pi(count)
      }
    } else {
      val sec = options.getOrElse('duration, 60).asInstanceOf[Int]
      println("=====================================================")
      println("Scheduled to run for at least " + sec + " sec")
      println("=====================================================")
      var time = System.currentTimeMillis
      val end = time + sec * 1000
      var i = 0
      while (time < end) {
        i += 1
        time = System.currentTimeMillis()
        println("======================================")
        println("Pre: Iteration " + i + " timestamp: " + time)
        println("======================================")

        calc_pi(count)

        time = System.currentTimeMillis()
        println("======================================")
        println("Post: Iteration " + i + " timestamp: " + time)
        println("======================================")
      }
    }
  }
}