package com.basho.riak.spark.metrics

import java.io.File
import scala.io.Source
import java.io.BufferedWriter
import java.io.FileWriter

/**
 * @author anekhaev
 */
object MetricsAggregatorApp extends App {
  
  val metricsDir = new File(args(0))
  val reportHeader = args(1)
  
  
  val metricsFiles = metricsDir.listFiles().toList.filter(f => f.isFile && f.getName.endsWith(".csv"))
  
  val metricsFilesByApp = metricsFiles.groupBy(f => f.getName.split('.')(0))
  
  metricsFilesByApp foreach { appMetricsFiles =>
    val (app, appFiles) = appMetricsFiles
    
    val metricsFilesByWorkers = appFiles.groupBy(f => f.getName.split('.')(1))
    
    val metricsByWorker = metricsFilesByWorkers map { workerMetricsFiles => 
        val (worker, workerFiles) = workerMetricsFiles
        
        val metrics = workerFiles.map { file =>
          val metricName = file.getName.split('.').drop(2).dropRight(1).mkString(".")
          val lastLine = Source.fromFile(file).getLines().foldLeft("")((acc, line) => line)
          Metric.parseFromCsvLine(metricName, lastLine)
        }
        //println(Metric.prettyPrint(metrics) + "\n\n")
        (worker, metrics)
    }
    
    metricsByWorker foreach { workerMetrics =>
      val (worker, metrics) = workerMetrics
      writeTextFile(new File(metricsDir, s"$app.$worker.stat"), Metric.prettyPrint(reportHeader, metrics))
    }
    
    val metricsByApp = metricsByWorker.map(_._2).flatten.toList.groupBy(_.name).map(_._2).map(Metric.averageMetric(_)).toList
    
    //println(Metric.prettyPrint(metricsByApp) + "\n\n")
    writeTextFile(new File(metricsDir, s"$app.stat"), Metric.prettyPrint(reportHeader, metricsByApp))
  }
  
  
  def writeTextFile(file: File, text: String) = {
    val bw = new BufferedWriter(new FileWriter(file))
    try {
      bw.write(text)
    } finally {
      bw.close()  
    }
  }
  
  
  case class Metric(
    name: String,
    count: Int,
    min: Double,
    mean: Double,
    max: Double,
    stdDev: Double) {
  }
  
  object Metric {
    
    def parseFromCsvLine(metricName: String, line: String): Metric = {
      val columns = line.split(',')
      val count = columns(1).toInt
      val max = columns(2).toDouble
      val mean = columns(3).toDouble
      val min = columns(4).toDouble
      val stdDev = columns(5).toDouble
      Metric(metricName, count, min, mean, max, stdDev)
    }
    
    def averageMetric(metrics: List[Metric]): Metric = {
      Metric(
        metrics.head.name,
        metrics.map(_.count).sum,
        metrics.map(_.min).sum / metrics.size,
        metrics.map(_.mean).sum / metrics.size,
        metrics.map(_.max).sum / metrics.size,
        metrics.map(_.stdDev).sum / metrics.size
      )
    }
    
    
    def prettyPrint(header: String, metrics: List[Metric]) = {
      
      def maxColumnLen(header: String, values: List[String]) = 
        (header :: values).map(_.length).max + 4
      
      def fmt(maxLen: Int, value: Any) = 
        String.format(s"%-${maxLen}s", "" + value)
       
          
      val nameLen = maxColumnLen("METRIC", metrics.map(_.name))
      val countLen = maxColumnLen("COUNT", metrics.map(_.count.toString))
      val minLen = maxColumnLen("MIN", metrics.map(_.min.toString))
      val meanLen = maxColumnLen("MEAN", metrics.map(_.mean.toString))
      val maxLen = maxColumnLen("MAX", metrics.map(_.max.toString))
      val stdevLen = maxColumnLen("STDDEV", metrics.map(_.stdDev.toString))
      
      val report = new StringBuilder()
      
      // Report header
      report.append(header)
      report.append("\n\n\n")
      
      // Table header
      report.append(fmt(nameLen, "METRIC"))
      report.append(fmt(countLen, "COUNT"))
      report.append(fmt(minLen, "MIN"))
      report.append(fmt(meanLen, "MEAN"))
      report.append(fmt(maxLen, "MAX"))
      report.append(fmt(stdevLen, "STDDEV"))
      report.append("\n")
      report.append("-" * (nameLen + countLen + minLen + meanLen + maxLen + stdevLen))
      report.append("\n")
      
      // Table rows
      metrics.sortBy(_.name).foreach { m => 
        report.append(fmt(nameLen, m.name))
        report.append(fmt(countLen, m.count))
        report.append(fmt(minLen, m.min))
        report.append(fmt(meanLen, m.mean))
        report.append(fmt(maxLen, m.max))
        report.append(fmt(stdevLen, m.stdDev))
        report.append("\n")
      }
      
      report.toString
    }
    
  }
  
}