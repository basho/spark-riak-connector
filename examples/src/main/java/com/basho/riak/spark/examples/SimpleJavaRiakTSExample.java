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
package com.basho.riak.spark.examples;

import com.basho.riak.spark.japi.rdd.RiakTSJavaRDD;
import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row$;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

import java.io.Serializable;
import java.util.Calendar;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;
import static java.util.Arrays.asList;
import static scala.collection.JavaConversions.asScalaBuffer;

/**
 * Really simple demo of timeseries-related features
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
public class SimpleJavaRiakTSExample implements Serializable {
  private static String TABLE_NAME = "ts_weather_demo";
  private static long START_TIME = 1443647460000L; // Thu Oct 01 00:11:00
  private static long SECOND = 1000L;
  private static long HALF_AN_HOUR = 30 * 60 * SECOND;
  private static List<WeatherDemo> DEMO_DATA = asList(
      new WeatherDemo(START_TIME, "rain", 20.0, 84.0, 30.06), 
      new WeatherDemo(START_TIME + 1 * HALF_AN_HOUR, "fair", 21.0, 76.0, 30.10), 
      new WeatherDemo(START_TIME + 2 * HALF_AN_HOUR, "wind", 22.0, 72.0, 30.17),
      new WeatherDemo(START_TIME + 3 * HALF_AN_HOUR, "sunny", 24.0, 35.0, 30.17), 
      new WeatherDemo(START_TIME + 4 * HALF_AN_HOUR, "sunny", 24.0, 37.0, 29.98), 
      new WeatherDemo(START_TIME + 5 * HALF_AN_HOUR, "a few clouds", 22.0, 39.0, 29.98), 
      new WeatherDemo(START_TIME + 6 * HALF_AN_HOUR, "a few clouds", 21.0, 42.0, 29.99), 
      new WeatherDemo(START_TIME + 7 * HALF_AN_HOUR, "mostly cloudy", 20.0, 44.0, 30.00));

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("Simple Java Riak Demo");

    setSparkOpt(sparkConf, "spark.master", "local");
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087");
    
    long from = beginingOfQuantumMillis(Iterables.getFirst(DEMO_DATA, null).getTime());
    long to = endOfQuantumMillis(Iterables.getLast(DEMO_DATA).getTime());

    final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    
    loadDemoData(jsc);
    
    RiakTSJavaRDD<GenericRowWithSchema> rdd = javaFunctions(jsc).riakTSTable(TABLE_NAME, GenericRowWithSchema.class)
      .sql(String.format("SELECT * FROM %s WHERE time >= %d AND time <= %d  AND  weather = 'sunny' AND family = 'f'", TABLE_NAME, from, to));

    rdd.foreach(x -> System.out.println(x.toString()));
  }

  protected static void loadDemoData(JavaSparkContext jsc) {
    List<org.apache.spark.sql.Row> rows = DEMO_DATA
        .stream()
        .map(
            x -> Row$.MODULE$.apply(asScalaBuffer(asList(x.getWeather(), "f", x.getTime(), x.getTemperature(), x.getHumidity(),
                x.getPressure())))).collect(Collectors.toList());
    JavaRDD<org.apache.spark.sql.Row> sqlRdd = jsc.parallelize(rows);

    javaFunctions(sqlRdd).saveToRiakTS(TABLE_NAME);
  }
  
  // Since quantum is equal to 1h, it begins at xx:00:00.000 
  private static long beginingOfQuantumMillis(long time) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTimeInMillis();
  }
  
  // Since quantum is equal to 1h, it ends at xx:59:59.999 
  private static long endOfQuantumMillis(long time) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(time);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 999);
    return cal.getTimeInMillis();
  }

  private static SparkConf setSparkOpt(SparkConf sparkConf, String option, String defaultOptVal) {
    try {
      sparkConf.getOption(option).get();
    } catch (NoSuchElementException ex) {
      sparkConf.set(option, defaultOptVal);
    }
    return sparkConf;
  }

  private static class WeatherDemo {
    private long time;
    private String weather;
    private double temperature;
    private double humidity;
    private double pressure;

    public WeatherDemo(long time, String weather, double temperature, double humidity, double pressure) {
      this.time = time;
      this.weather = weather;
      this.temperature = temperature;
      this.humidity = humidity;
      this.pressure = pressure;
    }

    public long getTime() {
      return time;
    }

    public String getWeather() {
      return weather;
    }

    public double getTemperature() {
      return temperature;
    }

    public double getHumidity() {
      return humidity;
    }

    public double getPressure() {
      return pressure;
    }

  }
}
