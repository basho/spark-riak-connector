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
package com.basho.spark.connector.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class AbstractJavaSparkTest extends AbstractRiakTest {
    // JavaSparkContext, created per test case
    protected JavaSparkContext jsc = null;

    protected SparkConf initSparkConf() {
        return new SparkConf(false)
                .setMaster("local")
                .setAppName(this.getClass().getSimpleName())
                .set("spark.riak.connection.host", DEFAULT_RIAK_HOST() + ":" + DEFAULT_RIAK_PORT())
                .set("spark.riak.output.wquorum", "1")
                .set("spark.riak.input.page.row.size", "2");
    }

    @Override
    public void initialize() {
        super.initialize();
        jsc = new JavaSparkContext(initSparkConf());
    }
}