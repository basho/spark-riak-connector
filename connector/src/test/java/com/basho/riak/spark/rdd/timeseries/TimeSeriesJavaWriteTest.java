/*******************************************************************************
 * Copyright (c) 2016 IBM Corp.
 * 
 * Created by Basho Technologies for IBM
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *******************************************************************************/
package com.basho.riak.spark.rdd.timeseries;

import com.basho.riak.spark.japi.rdd.RiakTSJavaRDD;
import com.basho.riak.spark.rdd.RiakTSTests;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType$;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.collection.Seq;

import java.util.List;
import java.util.stream.Collectors;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;
import static java.util.Arrays.asList;
import static scala.collection.JavaConversions.asScalaBuffer;

@Category(RiakTSTests.class)
public class TimeSeriesJavaWriteTest extends AbstractJavaTimeSeriesTest {

    public TimeSeriesJavaWriteTest() {
        super(false);
    }

    @Test
    public void saveSqlRowsToRiak() {
        JavaRDD<Row> sqlRowsRdd = jsc.parallelize(asList(
                Row$.MODULE$.apply(asScalaBuffer(asList(1, "f", 111111L, "bryce", 305.37))),
                Row$.MODULE$.apply(asScalaBuffer(asList(1, "f", 111222L, "bryce", 300.12))),
                Row$.MODULE$.apply(asScalaBuffer(asList(1, "f", 111333L, "bryce", 295.95))),
                Row$.MODULE$.apply(asScalaBuffer(asList(1, "f", 111444L, "ratman", 362.121))),
                Row$.MODULE$.apply(asScalaBuffer(asList(1, "f", 111555L, "ratman", 3502.212)))
        ));

        javaFunctions(sqlRowsRdd).saveToRiakTS(bucketName());

        RiakTSJavaRDD<Row> newRdd = javaFunctions(jsc)
                .riakTSTable(bucketName(), Row.class)
                .sql(String.format("SELECT user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()));

        List<Seq<Object>> data = newRdd.collect().stream().map(Row::toSeq).collect(Collectors.toList());
        assertEqualsUsingJSONIgnoreOrder(
                "[" +
                        "['bryce',305.37]," +
                        "['bryce',300.12]," +
                        "['bryce',295.95]," +
                        "['ratman',362.121]," +
                        "['ratman',3502.212]" +
                        "]"
                , data);
    }

    @Test
    public void saveDataFrameWithSchemaToRiak() {
        JavaRDD<String> jsonRdd = jsc.parallelize(asList(
                "{\"surrogate_key\": 1, \"family\": \"f\", \"time\": 111111, \"user_id\": \"bryce\", \"temperature_k\": 305.37}",
                "{\"surrogate_key\": 1, \"family\": \"f\", \"time\": 111222, \"user_id\": \"bryce\", \"temperature_k\": 300.12}",
                "{\"surrogate_key\": 1, \"family\": \"f\", \"time\": 111333, \"user_id\": \"bryce\", \"temperature_k\": 295.95}",
                "{\"surrogate_key\": 1, \"family\": \"f\", \"time\": 111444, \"user_id\": \"ratman\", \"temperature_k\": 362.121}",
                "{\"surrogate_key\": 1, \"family\": \"f\", \"time\": 111555, \"user_id\": \"ratman\", \"temperature_k\": 3502.212}"
        ));

        Dataset<Row> df = sparkSession().read().schema(StructType$.MODULE$.apply(asScalaBuffer(asList(
                DataTypes.createStructField("surrogate_key", DataTypes.IntegerType, true),
                DataTypes.createStructField("family", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("temperature_k", DataTypes.DoubleType, true)
        )))).json(jsonRdd);

        javaFunctions(df.javaRDD()).saveToRiakTS(bucketName());

        RiakTSJavaRDD<Row> newRdd = javaFunctions(jsc)
                .riakTSTable(bucketName(), Row.class)
                .sql(String.format("SELECT user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()));

        List<Seq<Object>> data = newRdd.collect().stream().map(Row::toSeq).collect(Collectors.toList());
        assertEqualsUsingJSONIgnoreOrder(
                "[" +
                        "['bryce',305.37]," +
                        "['bryce',300.12]," +
                        "['bryce',295.95]," +
                        "['ratman',362.121]," +
                        "['ratman',3502.212]" +
                        "]"
                , data);
    }
}
