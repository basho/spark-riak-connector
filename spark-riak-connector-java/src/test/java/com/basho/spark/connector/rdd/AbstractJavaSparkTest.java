package com.basho.spark.connector.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractJavaSparkTest extends AbstractRiakTest {
    // JavaSparkContext, created per test case
    protected JavaSparkContext jsc = null;

    protected SparkConf initSparkConf() {
        return new SparkConf(false)
                .setMaster("local")
                .setAppName(this.getClass().getSimpleName())
                .set("spark.riak.connection.host", DEFAULT_RIAK_HOST() + ":" + DEFAULT_RIAK_PORT())
                .set("spark.riak.output.wquorum", "1")
                .set("spark.riak.input.page.row.size", "2");
    }

    @Override
    public void initialize() {
        super.initialize();
        jsc = new JavaSparkContext(initSparkConf());
    }
}