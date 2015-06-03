package com.basho.spark.connector.japi.rdd;

import com.basho.spark.connector.rdd.RiakRDD;
import com.basho.spark.connector.util.JavaApiHelper;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import static com.basho.spark.connector.util.JavaApiHelper.getClassTag;

public class RiakJavaPairRDD<K, V> extends JavaPairRDD<K, V> {
    private ClassTag<K> kClassTag;
    private ClassTag<V> vClassTag;

    public RiakJavaPairRDD(
            RiakRDD<Tuple2<K, V>> rdd,
            Class<K> keyClass,
            Class<V> valueClass){
        this(rdd, getClassTag(keyClass), getClassTag(valueClass));
    }

    public RiakJavaPairRDD(
            RiakRDD<Tuple2<K, V>> rdd,
            ClassTag<K> keyClassTag,
            ClassTag<V> valueClassTag){
        super(rdd, keyClassTag, valueClassTag);
        this.kClassTag = keyClassTag;
        this.vClassTag = valueClassTag;
    }

    @Override
    public RiakRDD<Tuple2<K, V>> rdd() {
        return (RiakRDD<Tuple2<K, V>>) super.rdd();
    }

    private RiakJavaPairRDD<K, V> wrap(RiakRDD<Tuple2<K, V>> newRDD) {
        return new RiakJavaPairRDD<>(newRDD, kClassTag(), vClassTag());
    }

    @Override
    public ClassTag<V> vClassTag() {
        return vClassTag;
    }

    @Override
    public ClassTag<K> kClassTag() {
        return kClassTag;
    }

    public RiakJavaPairRDD<K, V> query2iRange(String index, Long from, Long to){
        return wrap(rdd().query2iRange(index, from, to));
    }

    public RiakJavaPairRDD<K, V> queryBucketKeys(String... keys){
        return wrap(rdd().queryBucketKeys(JavaApiHelper.toScalaSeq(keys)));
    }
}
