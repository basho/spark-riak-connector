/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.examples

import com.basho.riak.client.core.operations.ts.StoreOperation

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.basho.riak.client.core.query.timeseries.Cell
import com.basho.riak.client.core.query.timeseries.Row
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.RiakFunctions
import com.basho.riak.spark.toSparkContextFunctions
import java.util.Calendar

/**
 * Really simple demo timeseries-related features
 * CREATE TABLE ts_weather_demo
 *  (
 *     weather      varchar not null,
 *     family       varchar not null,
 *     time         timestamp not null,
 *     temperature  double,
 *     humidity     double,
 *     pressure     double,
 *     PRIMARY KEY (
 *         (weather, family, quantum(time, 1, 'h')), weather, family, time
 *     )
 *  )
 */
object SimpleScalaRiakTSExample {

  case class WeatherDemo(time: Long, weather: String, temperature: Double, humidity: Double, pressure: Double)

  private val tableName = "ts_weather_demo"
  private val startTime = 1443647460000l // Thu Oct 01 00:11:00
  private val second = 1000l
  private val halfAnHour = 30 * 60 * second
  private val quantum = 2 * halfAnHour // 1 hour

  val testData = List(
    WeatherDemo(startTime, "rain", 20.0, 84.0, 30.06),
    WeatherDemo(startTime + 1 * halfAnHour, "fair", 21.0, 76.0, 30.10),
    WeatherDemo(startTime + 2 * halfAnHour, "wind", 22.0, 72.0, 30.17),
    WeatherDemo(startTime + 3 * halfAnHour, "sunny", 24.0, 35.0, 30.17),
    WeatherDemo(startTime + 4 * halfAnHour, "sunny", 24.0, 37.0, 29.98),
    WeatherDemo(startTime + 5 * halfAnHour, "a few clouds", 22.0, 39.0, 29.98),
    WeatherDemo(startTime + 6 * halfAnHour, "a few clouds", 21.0, 42.0, 29.99),
    WeatherDemo(startTime + 7 * halfAnHour, "mostly cloudy", 20.0, 44.0, 30.00))

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Simple Scala Riak TS Demo")

    setSparkOpt(sparkConf, "spark.master", "local")
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087")

    loadDemoData(sparkConf)
    val sc = new SparkContext(sparkConf)
    
    val from = beginingOfQuantumMillis(testData.head.time)
    val to = endOfQuantumMillis(testData.last.time)

    val rdd = sc.riakTSBucket[org.apache.spark.sql.Row](tableName)
      .sql(s"SELECT * FROM $tableName WHERE time >= $from AND time <= $to  AND  weather != 'sunny' AND family = 'f'")
    println(s"Execution result: ${rdd.count}")

    // Filter data using query language rather than filter over rdd
    val filteredRdd = sc.riakTSBucket(tableName)
      .sql(s"SELECT * FROM $tableName WHERE time >= $from AND time <= $to AND weather = 'sunny' AND family = 'f'")
    println(s"Execution result with filtering: ${filteredRdd.count}")
  }

  private def loadDemoData(sparkConf: SparkConf): Unit = {
    val rf = RiakFunctions(sparkConf)

    val rows = testData.map(x => new Row(
          new Cell(x.weather),
          new Cell("f") /* family */,
          Cell.newTimestamp(x.time),
          new Cell(x.temperature),
          new Cell(x.humidity),
          new Cell(x.pressure)
        )
      )

    val storeOp = new StoreOperation.Builder(BinaryValue.create(tableName)).withRows(rows).build

    rf.withRiakDo(session => {
      val r = session.getRiakCluster.execute(storeOp).get
      assert(true)
    })
  }

  private def setSparkOpt(sparkConf: SparkConf, option: String, defaultOptVal: String): SparkConf = {
    val optval = sparkConf.getOption(option).getOrElse(defaultOptVal)
    sparkConf.set(option, optval)
  }
  
  // Since quantum is equal to 1h, it begins at xx:00:00.000 
  private def beginingOfQuantumMillis(time: Long): Long = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTimeInMillis
  }
  
  // Since quantum is equal to 1h, it ends at xx:59:59.999 
  private def endOfQuantumMillis(time: Long): Long = {
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.set(Calendar.MINUTE, 59)
    cal.set(Calendar.SECOND, 59)
    cal.set(Calendar.MILLISECOND, 999)
    cal.getTimeInMillis
  }
}
