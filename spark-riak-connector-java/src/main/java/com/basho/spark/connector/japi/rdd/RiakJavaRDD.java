package com.basho.spark.connector.japi.rdd;

import com.basho.spark.connector.rdd.RiakRDD;
import com.basho.spark.connector.util.JavaApiHelper;
import org.apache.spark.api.java.JavaRDD;
import static com.basho.spark.connector.util.JavaApiHelper.getClassTag;

import scala.reflect.ClassTag;

public class RiakJavaRDD<T> extends JavaRDD<T> {
    private ClassTag<T> classTag;

    public RiakJavaRDD(RiakRDD<T> rdd, Class<T> clazz) {
        this(rdd, getClassTag(clazz));
    }

    public RiakJavaRDD(RiakRDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
        this.classTag = classTag;
    }

    @Override
    public RiakRDD<T> rdd() {
        return (RiakRDD<T>) super.rdd();
    }

    private RiakJavaRDD<T> wrap(RiakRDD<T> newRDD) {
        return new RiakJavaRDD<>(newRDD, classTag());
    }

    @Override
    public ClassTag<T> classTag() {
        return classTag;
    }

    public RiakJavaRDD<T> query2iRange(String index, Long from, Long to){
        return wrap(rdd().query2iRange(index, from, to));
    }

    public RiakJavaRDD<T> queryBucketKeys(String... keys){
        return wrap(rdd().queryBucketKeys(JavaApiHelper.toScalaSeq(keys)));
    }
}
