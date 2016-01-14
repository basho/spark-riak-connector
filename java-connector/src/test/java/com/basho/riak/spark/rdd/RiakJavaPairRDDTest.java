package com.basho.riak.spark.rdd;

import com.basho.riak.spark.japi.rdd.RiakJavaPairRDD;
import org.junit.Test;
import scala.Tuple2;
import scala.reflect.ClassTag;

import static com.basho.riak.spark.util.JavaApiHelper.getClassTag;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class RiakJavaPairRDDTest {

    /*
     * Unit test for verifying logic after removing redundant ClassTag fields for key and value from
     * com.basho.riak.spark.japi.rdd.RiakJavaPairRDD class
     */
    @SuppressWarnings("unchecked")
    @Test
    public void createWithClassTags() {
        final ClassTag<String> kClassTag = getClassTag(String.class);
        final ClassTag<Object> vClassTag = getClassTag(Object.class);
        final RiakRDD<Tuple2<String, Object>> rdd = mock(RiakRDD.class);

        RiakJavaPairRDD<String, Object> pairRDD = new RiakJavaPairRDD<>(rdd, kClassTag, vClassTag);
        assertEquals(kClassTag, pairRDD.kClassTag());
        assertEquals(vClassTag, pairRDD.vClassTag());
    }

    /*
     * Unit test for verifying logic after removing redundant ClassTag fields for key and value from
     * com.basho.riak.spark.japi.rdd.RiakJavaPairRDD class
     */
    @SuppressWarnings("unchecked")
    @Test
    public void createWithClass() {
        final RiakRDD<Tuple2<String, Object>> rdd = mock(RiakRDD.class);

        RiakJavaPairRDD<String, Object> pairRDD = new RiakJavaPairRDD<>(rdd, String.class, Object.class);
        assertEquals(getClassTag(String.class), pairRDD.kClassTag());
        assertEquals(getClassTag(Object.class), pairRDD.vClassTag());
    }
}
