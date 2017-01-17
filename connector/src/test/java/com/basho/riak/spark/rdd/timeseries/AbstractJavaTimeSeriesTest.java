package com.basho.riak.spark.rdd.timeseries;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractJavaTimeSeriesTest extends AbstractTimeSeriesTest {

    // JavaSparkContext, created per test case
    protected JavaSparkContext jsc = new JavaSparkContext(sparkSession().sparkContext());

    public AbstractJavaTimeSeriesTest(boolean createTestDate) {
        super(createTestDate);
    }

    protected String stringify(String[] strings) {
        return "[" + StringUtils.join(strings, ",") + "]";
    }
}
