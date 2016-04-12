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
package com.basho.riak.spark.japi;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.spark.japi.rdd.RiakJavaPairRDD;
import com.basho.riak.spark.japi.rdd.RiakJavaRDD;
import com.basho.riak.spark.japi.rdd.RiakTSJavaRDD;
import com.basho.riak.spark.rdd.*;
import com.basho.riak.spark.rdd.connector.RiakConnector$;
import com.basho.riak.spark.util.RiakObjectConversionUtil;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;
import scala.Function2;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

import static com.basho.riak.spark.util.JavaApiHelper.getClassTag;

public class SparkContextJavaFunctions {

    protected static class ConversionFunction<T> extends AbstractFunction2<Location, RiakObject, T> implements Serializable {
        private final ClassTag<T> classTag;

        public ConversionFunction(ClassTag<T> classTag) {
            this.classTag = classTag;
        }

        @Override
        public T apply(Location l, RiakObject r) {
            return RiakObjectConversionUtil.from(l, r, classTag);
        }

        public static <T> ConversionFunction create(ClassTag<T> classTag) {
            return new ConversionFunction<>(classTag);
        }
    }

    public final SparkContext sparkContext;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public <T> RiakJavaRDD<T> toJavaRDD(RiakRDD<T> rdd, Class<T> targetClass) {
        return new RiakJavaRDD<>(rdd, getClassTag(targetClass));
    }

    public <T> RiakJavaRDD<T> riakBucket(Namespace ns, Class<T> valueClass) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString(), valueClass);
    }

    @SuppressWarnings("unchecked")
    public <T> RiakJavaRDD<T> riakBucket(String bucketName, String bucketType, Class<T> valueClass) {
        final ClassTag<T> classTag = getClassTag(valueClass);
        final String bucketTypeStr = bucketType == null || bucketType.isEmpty() ? "default" : bucketType;
        final RiakRDD<T> rdd = RiakRDD$.MODULE$.apply(sparkContext, bucketTypeStr, bucketName,
                ConversionFunction.create(classTag), Option.apply(null), ReadConf$.MODULE$.apply(sparkContext.getConf()), classTag);
        return new RiakJavaRDD<>(rdd, classTag);
    }

    public <K, V> RiakJavaPairRDD<K, V> riakBucket(Namespace ns, Function2<Location, RiakObject, Tuple2<K, V>> convert, Class<K> keyClass, Class<V> valueClass) {
        return this.riakBucket(ns.getBucketNameAsString(), convert, ns.getBucketTypeAsString(), keyClass, valueClass);
    }

    public <K, V> RiakJavaPairRDD<K, V> riakBucket(String bucketName, Function2<Location, RiakObject, Tuple2<K, V>> convert, String bucketType, Class<K> keyClass, Class<V> valueClass) {
        final ClassTag<K> kClassTag = getClassTag(keyClass);
        final ClassTag<V> vClassTag = getClassTag(valueClass);
        final String bucketTypeStr = bucketType == null || bucketType.isEmpty() ? "default" : bucketType;
        final RiakRDD<Tuple2<K, V>> rdd = RiakRDD$.MODULE$.apply(sparkContext, bucketTypeStr, bucketName, convert,
                Option.apply(null), ReadConf$.MODULE$.apply(sparkContext.getConf()), kClassTag, vClassTag);
        return new RiakJavaPairRDD<>(rdd, kClassTag, vClassTag);
    }

    public <T> RiakTSJavaRDD<T> riakTSTable(String bucketName, Class<T> targetClass) {
        final ClassTag<T> classTag = getClassTag(targetClass);
        final RiakTSRDD<T> rdd = RiakTSRDD$.MODULE$.apply(sparkContext, bucketName, ReadConf$.MODULE$.apply(sparkContext.getConf()),
            classTag, RiakConnector$.MODULE$.apply(sparkContext.getConf()));
        return new RiakTSJavaRDD<>(rdd, classTag);
    }

    public <T> RiakTSJavaRDD<T> riakTSTable(String bucketName, StructType schema, Class<T> targetClass) {
        final ClassTag<T> classTag = getClassTag(targetClass);
        final RiakTSRDD<T> rdd = RiakTSRDD$.MODULE$.apply(sparkContext, bucketName,
                ReadConf$.MODULE$.apply(sparkContext.getConf()), Option.apply(schema),
                classTag, RiakConnector$.MODULE$.apply(sparkContext.getConf()));
        return new RiakTSJavaRDD<>(rdd, classTag);
    }
}
