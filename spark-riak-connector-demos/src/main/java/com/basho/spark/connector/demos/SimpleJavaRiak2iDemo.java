package com.basho.spark.connector.demos;

import com.basho.riak.client.core.query.Namespace;
import com.basho.spark.connector.japi.SparkJavaUtil;
import com.basho.spark.connector.japi.rdd.RiakJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class SimpleJavaRiak2iDemo implements Serializable {

    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        String indexName = sc.get("spark.riak.demo.index", "creationIndex");
        String bucketName = sc.get("spark.riak.demo.bucket", "test-bucket");
        String from = sc.get("spark.riak.demo.from", "1");
        String to = sc.get("spark.riak.demo.to", "4");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        RiakJavaRDD<String> rdd = SparkJavaUtil.javaFunctions(jsc).riakBucket(new Namespace(bucketName), String.class).
                                  query2iRange(indexName, Long.getLong(from), Long.getLong(to));
        System.out.println(String.format("Execution result: %s", rdd.count()));
    }
}
