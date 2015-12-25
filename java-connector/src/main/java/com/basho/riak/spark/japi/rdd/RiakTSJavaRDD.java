/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.japi.rdd;

import static com.basho.riak.spark.util.JavaApiHelper.getClassTag;

import org.apache.spark.api.java.JavaRDD;

import scala.reflect.ClassTag;

import com.basho.riak.spark.rdd.RiakTSRDD;

public class RiakTSJavaRDD<T> extends JavaRDD<T> {

    private ClassTag<T> classTag;

    public RiakTSJavaRDD(RiakTSRDD<T> rdd, Class<T> clazz) {
        this(rdd, getClassTag(clazz));
    }

    public RiakTSJavaRDD(RiakTSRDD<T> rdd, ClassTag<T> classTag) {
        super(rdd, classTag);
        this.classTag = classTag;
    }

    @Override
    public RiakTSRDD<T> rdd() {
        return (RiakTSRDD<T>) super.rdd();
    }

    private RiakTSJavaRDD<T> wrap(RiakTSRDD<T> newRDD) {
        return new RiakTSJavaRDD<>(newRDD, classTag());
    }

    @Override
    public ClassTag<T> classTag() {
        return classTag;
    }

    public RiakTSJavaRDD<T> sql(String queryString) {
        return wrap(rdd().sql(queryString));
    }

}
