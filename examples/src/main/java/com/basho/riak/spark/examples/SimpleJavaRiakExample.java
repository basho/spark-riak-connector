package com.basho.riak.spark.examples;

import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.spark.japi.SparkJavaUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;

/**
 * Really simple demo program which calculates the number of records loaded from the Riak bucket
 */
public class SimpleJavaRiakExample implements Serializable {
  private static final String SOURCE_DATA = "test-data";
  private static final String TEST_DATA = "[" + "  {key: 'key-1', creationNo: 1, value: 'value1'}"
      + ", {key: 'key-2', creationNo: 2, value: 'value2'}" 
      + ", {key: 'key-3', creationNo: 3, value: 'value3'}"
      + ", {key: 'key-4', creationNo: 4, value: 'value4'}" 
      + ", {key: 'key-5', creationNo: 5, value: 'value5'}"
      + ", {key: 'key-6', creationNo: 6, value: 'value6'}" 
      + "]";

  private static final ObjectMapper tolerantMapper = new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
          .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

  public static void main(String[] args) throws IOException {
    SparkConf sparkConf = new SparkConf().setAppName("Simple Java Riak Demo");

    setSparkOpt(sparkConf, "spark.master", "local");
    setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087");


    final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    createDemoData(jsc);

    final List<String> allValues = SparkJavaUtil.javaFunctions(jsc).riakBucket(SOURCE_DATA, String.class).queryAll().collect();
    System.out.println("All values:");
    allValues.forEach(System.out::println);
    System.out.println("\n");

    final List<String> filteredBy2iRangeLocal = SparkJavaUtil.javaFunctions(jsc).riakBucket(SOURCE_DATA, String.class).query2iRangeLocal("creationNo", 2L, 5L).collect();
    System.out.println("Values filtered by secondary index range local:");
    filteredBy2iRangeLocal.forEach(System.out::println);
    System.out.println("\n");

    final List<String> filteredBy2iRange = SparkJavaUtil.javaFunctions(jsc).riakBucket(SOURCE_DATA, String.class).query2iRange("creationNo", 2L, 5L).collect();
    System.out.println("Values filtered by secondary index range:");
    filteredBy2iRange.forEach(System.out::println);
    System.out.println("\n");

    final List<String> filteredByKeys = SparkJavaUtil.javaFunctions(jsc).riakBucket(SOURCE_DATA, String.class).queryBucketKeys("key-1", "key-3", "key-6").collect();
    System.out.println("Values filtered by keys:");
    filteredByKeys.forEach(System.out::println);
    System.out.println("\n");

    final List<String> filteredBy2iKeys = SparkJavaUtil.javaFunctions(jsc).riakBucket(SOURCE_DATA, String.class).query2iKeys("creationNo", 1L, 3L, 6L).collect();
    System.out.println("Values filtered by 2i keys:");
    filteredBy2iKeys.forEach(System.out::println);
    System.out.println("\n");
  }



  private static void createDemoData(JavaSparkContext jsc) throws IOException {
    List<Demo> vals = tolerantMapper.readValue(TEST_DATA, new TypeReference<List<Demo>>() {});
    JavaRDD<Demo> rdd = jsc.parallelize(vals);
    javaFunctions(rdd).saveToRiak("test-data");
  }

  private static SparkConf setSparkOpt(SparkConf sparkConf, String option, String defaultOptVal) {
    try {
      sparkConf.getOption(option).get();
    } catch (NoSuchElementException ex) {
      sparkConf.set(option, defaultOptVal);
    }
    return sparkConf;
  }
  
  private static class Demo implements Serializable{
    @RiakIndex(name = "creationNo")
    private long creationNo;
    private String value;
    @RiakKey
    private String key;
    
    public Demo() {
    }
    
    public Demo(long creationNo, String value, String key) {
      this.creationNo = creationNo;
      this.value = value;
      this.key = key;
    }
    public long getCreationNo() {
      return creationNo;
    }
    public void setCreationNo(long creationNo) {
      this.creationNo = creationNo;
    }
    public String getValue() {
      return value;
    }
    public void setValue(String value) {
      this.value = value;
    }
    public String getKey() {
      return key;
    }
    public void setKey(String key) {
      this.key = key;
    }
  }
}
