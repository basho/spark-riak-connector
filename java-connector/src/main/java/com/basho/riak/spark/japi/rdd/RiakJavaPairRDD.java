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
package com.basho.riak.spark.japi.rdd;

import com.basho.riak.spark.rdd.RiakRDD;
import com.basho.riak.spark.util.JavaApiHelper;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.reflect.ClassTag;
import static com.basho.riak.spark.util.JavaApiHelper.getClassTag;

public class RiakJavaPairRDD<K, V> extends JavaPairRDD<K, V> {

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
    }

    @Override
    public RiakRDD<Tuple2<K, V>> rdd() {
        return (RiakRDD<Tuple2<K, V>>) super.rdd();
    }

    private RiakJavaPairRDD<K, V> wrap(RiakRDD<Tuple2<K, V>> newRDD) {
        return new RiakJavaPairRDD<>(newRDD, kClassTag(), vClassTag());
    }

    public RiakJavaPairRDD<K, V> query2iRange(String index, Long from, Long to){
        return wrap(rdd().query2iRange(index, from, to));
    }

    public RiakJavaPairRDD<K, V> queryBucketKeys(String... keys){
        return wrap(rdd().queryBucketKeys(JavaApiHelper.toScalaSeq(keys)));
    }
}
