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

import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.spark.japi.rdd.RiakJavaPairRDD;
import com.basho.riak.spark.japi.rdd.RiakJavaRDD;
import com.basho.riak.spark.japi.rdd.RiakTSJavaRDD;
import com.basho.riak.spark.japi.rdd.mapper.DefaultReadDataMapper;
import com.basho.riak.spark.query.QueryData;
import com.basho.riak.spark.rdd.*;
import com.basho.riak.spark.rdd.connector.RiakConnector$;
import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory;
import com.basho.riak.spark.rdd.mapper.ReadPairValueDataMapper$;
import com.basho.riak.spark.rdd.mapper.ReadValueDataMapper$;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.reflect.ClassTag;

import static com.basho.riak.spark.util.JavaApiHelper.getClassTag;

public class SparkContextJavaFunctions {

    public final SparkContext sparkContext;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public <T> RiakJavaRDD<T> toJavaRDD(RiakRDD<T> rdd, Class<T> targetClass) {
        return new RiakJavaRDD<>(rdd, getClassTag(targetClass));
    }

    public <K, V> RiakJavaPairRDD<K, V> toJavaPairRDD(RiakRDD<Tuple2<K, V>> rdd, Class<K> keyClass, Class<V> valueClass) {
        return new RiakJavaPairRDD<>(rdd, getClassTag(keyClass), getClassTag(valueClass));
    }

    public <T> RiakJavaRDD<T> riakBucket(String bucketName, String bucketType, ReadDataMapperFactory<T> rdmf) {
        final String bucketTypeStr = StringUtils.isBlank(bucketType) ? "default" : bucketType;
        final Option<QueryData<?>> queryData = Option.apply(null);
        final ReadConf readConf = ReadConf$.MODULE$.apply(sparkContext.getConf());

        final RiakRDD<T> rdd = RiakRDD$.MODULE$.apply(sparkContext, bucketTypeStr,
                bucketName, queryData, readConf, getClassTag(rdmf.targetClass()), rdmf);
        return toJavaRDD(rdd, rdmf.targetClass());
    }

    public <T> RiakJavaRDD<T> riakBucket(String bucketName, Class<T> valueClass) {
        return riakBucket(bucketName, "default", valueClass);
    }

    public <T> RiakJavaRDD<T> riakBucket(String bucketName, String bucketType, Class<T> valueClass) {
        final ReadDataMapperFactory<T> rdmf = ReadValueDataMapper$.MODULE$.factory(getClassTag(valueClass));
        return this.riakBucket(bucketName, bucketType, rdmf);
    }

    public <T> RiakJavaRDD<T> riakBucket(Namespace ns, ReadDataMapperFactory<T> rdmf) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString(), rdmf);
    }

    public <T> RiakJavaRDD<T> riakBucket(Namespace ns, Class<T> valueClass) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString(), valueClass);
    }

    public RiakJavaRDD<Tuple2<String, Object>> riakBucket(Namespace ns) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString());
    }

    public RiakJavaRDD<Tuple2<String, Object>> riakBucket(String bucketName, String bucketType) {
        return this.riakBucket(bucketName, bucketType, DefaultReadDataMapper.instance);
    }

    public <K extends String, V> RiakJavaPairRDD<K, V> riakBucket(String bucketName,
                                                                  String bucketType,
                                                                  Class<K> keyClass,
                                                                  Class<V> valueClass,
                                                                  ReadDataMapperFactory<Tuple2<K, V>> rdmf) {
        return toJavaPairRDD(this.riakBucket(bucketName, bucketType, rdmf).rdd(), keyClass, valueClass);
    }

    public <K extends String, V> RiakJavaPairRDD<K, V> riakBucket(String bucketName,
                                                                  String bucketType,
                                                                  Class<K> keyClass,
                                                                  Class<V> valueClass) {
        ReadDataMapperFactory<Tuple2<K, V>> rdmf = ReadPairValueDataMapper$.MODULE$
                .factory(getClassTag(keyClass), getClassTag(valueClass));
        return this.riakBucket(bucketName, bucketType, keyClass, valueClass, rdmf);
    }

    public <K extends String, V> RiakJavaPairRDD<K, V> riakBucket(Namespace ns,
                                                                  Class<K> keyClass,
                                                                  Class<V> valueClass,
                                                                  ReadDataMapperFactory<Tuple2<K, V>> rdmf) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString(), keyClass, valueClass, rdmf);
    }

    public <K extends String, V> RiakJavaPairRDD<K, V> riakBucket(Namespace ns,
                                                                  Class<K> keyClass,
                                                                  Class<V> valueClass) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString(), keyClass, valueClass);
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
