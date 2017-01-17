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
package com.basho.riak.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public abstract class AbstractJavaSparkTest extends AbstractRiakSparkTest {
    // JavaSparkContext, created per test case
    protected JavaSparkContext jsc = new JavaSparkContext(sparkSession().sparkContext());

    protected static class FuncReMapWithPartitionIdx<T> implements Function2<Integer, Iterator<T>, Iterator<Tuple2<Integer, T>>> {
        @Override
        public Iterator<Tuple2<Integer, T>> call(Integer ix, Iterator<T> it) throws Exception {
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
                    .map(x -> new Tuple2<>(ix, x)).iterator();
        }
    }

    protected static class FuncMapTupleToJavaPairRdd<K, V> implements PairFunction<Tuple2<K, V>, K, V> {
        @Override
        public Tuple2<K, V> call(Tuple2<K, V> t) throws Exception {
            return t;
        }
    }

    @SuppressWarnings("unused")
    protected static class UserTS implements Serializable {
        public String timestamp;
        public String user_id;
    }
}