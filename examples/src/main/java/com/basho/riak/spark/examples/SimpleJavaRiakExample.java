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
package com.basho.riak.spark.examples;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.spark.japi.SparkJavaUtil;
import com.basho.riak.spark.japi.rdd.RiakJavaRDD;
import com.basho.riak.spark.rdd.RiakFunctions;
import com.basho.riak.spark.rdd.RiakFunctions$;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.NoSuchElementException;

/**
 * Really simple demo program which calculates the number of records loaded
 * from the Riak using 2i range query
 */
public class SimpleJavaRiakExample implements Serializable {
    private static final Namespace SOURCE_DATA = new Namespace("test-data");
    private static final String TEST_DATA =
        "[" +
                "  {key: 'key-1', indexes: {creationNo: 1}, value: 'value1'}" +
                ", {key: 'key-2', indexes: {creationNo: 2}, value: 'value2'}" +
                ", {key: 'key-3', indexes: {creationNo: 3}, value: 'value3'}" +
                ", {key: 'key-4', indexes: {creationNo: 4}, value: 'value4'}" +
                ", {key: 'key-5', indexes: {creationNo: 5}, value: 'value5'}" +
                ", {key: 'key-6', indexes: {creationNo: 6}, value: 'value6'}" +
        "]";


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Simple Java Riak Demo");

        setSparkOpt(sparkConf, "spark.master", "local");
        setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:8087");
        //setSparkOpt(sparkConf, "spark.riak.connection.host", "127.0.0.1:10017");

        creteTestData(sparkConf);

        final JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        final RiakJavaRDD<String> rdd = SparkJavaUtil.javaFunctions(jsc).riakBucket(SOURCE_DATA, String.class).
                                  query2iRange("creationNo", 0L, 100L);

        System.out.println(String.format("Execution result: %s", rdd.count()));
    }

    protected static void creteTestData(SparkConf sparkConf) {
        final RiakFunctions rf = RiakFunctions$.MODULE$.apply(sparkConf);

        rf.withRiakDo(new AbstractFunction1<RiakClient, Object>() {
            @Override
            public Void apply(RiakClient client) {
                rf.createValues(client, SOURCE_DATA, TEST_DATA, true);
                return null;
            }
        });
    }

    private static SparkConf setSparkOpt(SparkConf sparkConf, String option, String defaultOptVal){
        try {
            sparkConf.getOption(option).get();
        } catch( NoSuchElementException ex){
            sparkConf.set(option, defaultOptVal);
        }
        return sparkConf;
    }
}
