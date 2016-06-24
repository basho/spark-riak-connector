package com.basho.riak.spark.rdd.timeseries;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.spark.util.RiakObjectConversionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import scala.reflect.ClassTag;

public abstract class AbstractJavaTimeSeriesTest extends AbstractTimeSeriesTest {

    // JavaSparkContext, created per test case
    protected JavaSparkContext jsc = null;

    public AbstractJavaTimeSeriesTest(boolean createTestDate) {
        super(createTestDate);
    }

    @Override
    public SparkContext createSparkContext(SparkConf conf) {
        final SparkContext sc = new SparkContext(conf);
        jsc = new JavaSparkContext(sc);
        return sc;
    }

    protected String stringify(String[] strings) {
        return "[" + StringUtils.join(strings, ",") + "]";
    }

    @Override
    public <T> T convertRiakObject(Location l, RiakObject ro, ClassTag<T> ev) {
        return RiakObjectConversionUtil.from(l, ro, ev);
    }
}
