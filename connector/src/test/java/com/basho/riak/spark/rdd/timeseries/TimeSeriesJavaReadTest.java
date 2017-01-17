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

import com.basho.riak.spark.rdd.RiakTSTests;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.riak.types.RiakStructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.None$;
import scala.Option;
import scala.Some$;
import scala.collection.Seq;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;

@Category(RiakTSTests.class)
public class TimeSeriesJavaReadTest extends AbstractJavaTimeSeriesTest {

    public TimeSeriesJavaReadTest() {
        super(true);
    }

    @Test
    public void readDataAsSqlRow() {
        List<Row> rows = javaFunctions(jsc)
                .riakTSTable(bucketName(), Row.class)
                .sql(String.format("SELECT user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()))
                .collect();
        List<Seq<Object>> data = rows.stream().map(Row::toSeq).collect(Collectors.toList());
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
    public void riakTSRDDToDataFrame() {
        JavaRDD<TimeSeriesDataBean> rows = javaFunctions(jsc)
                .riakTSTable(bucketName(), Row.class)
                .sql(String.format("SELECT time, user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()))
                .map(r -> new TimeSeriesDataBean(r.getTimestamp(0).getTime(), r.getString(1), r.getDouble(2)));

        Dataset<Row> df = sparkSession().createDataFrame(rows, TimeSeriesDataBean.class);
        df.createOrReplaceTempView("test");

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) sparkSession().sql("select * from test").toJSON().collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{time:111111, user_id:'bryce', temperature_k:305.37}," +
                "{time:111222, user_id:'bryce', temperature_k:300.12}," +
                "{time:111333, user_id:'bryce', temperature_k:295.95}," +
                "{time:111444, user_id:'ratman',temperature_k:362.121}," +
                "{time:111555, user_id:'ratman',temperature_k:3502.212}" +
                "]", stringify(data));
    }

    @Test
    public void riakTSRDDToDataFrameConvertTimestamp() {
        StructType structType = new StructType(new StructField[]{
                DataTypes.createStructField("time", DataTypes.LongType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("temperature_k", DataTypes.DoubleType, true),
        });

        JavaRDD<TimeSeriesDataBean> rows = javaFunctions(jsc)
                .riakTSTable(bucketName(), structType, Row.class)
                .sql(String.format("SELECT time, user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()))
                .map(r -> new TimeSeriesDataBean(r.getLong(0), r.getString(1), r.getDouble(2)));

        Dataset<Row> df = sparkSession().createDataFrame(rows, TimeSeriesDataBean.class);
        df.createOrReplaceTempView("test");

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) sparkSession().sql("select * from test").toJSON().collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{time:111111, user_id:'bryce', temperature_k:305.37}," +
                "{time:111222, user_id:'bryce', temperature_k:300.12}," +
                "{time:111333, user_id:'bryce', temperature_k:295.95}," +
                "{time:111444, user_id:'ratman',temperature_k:362.121}," +
                "{time:111555, user_id:'ratman',temperature_k:3502.212}" +
                "]", stringify(data));
    }

    @Test
    public void dataFrameGenericLoad() {
        sparkSession().udf().register("getMillis", (UDF1<Timestamp, Object>) Timestamp::getTime, DataTypes.LongType);

        Dataset<Row> df = sparkSession().read()
                .format("org.apache.spark.sql.riak")
                .schema(schema())
                .load(bucketName())
                .filter(String.format("time >= CAST('%s' AS TIMESTAMP) AND time <= CAST('%s' AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'", fromStr(), toStr()));
        df = df.select(functions.callUDF("getMillis", df.col("time")).as("time"), df.col("family"), df.col("surrogate_key"), df.col("user_id"), df.col("temperature_k"));

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) df.toJSON().collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37}," +
                "{surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12}," +
                "{surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95}," +
                "{surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121}," +
                "{surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212} " +
                "]", stringify(data));
    }

    @Test
    public void dataFrameReadShouldConvertTimestampToLong() {
        StructField[] structFields = {
                DataTypes.createStructField("surrogate_key", DataTypes.LongType, true),
                DataTypes.createStructField("family", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("temperature_k", DataTypes.DoubleType, true),
        };

        Dataset<Row> df = sparkSession().read()
                .option("spark.riak.partitioning.ts-range-field-name", "time")
                .format("org.apache.spark.sql.riak")
                .schema(new RiakStructType(structFields, scala.Option.empty(), scala.Option.apply("time")))
                .load(bucketName())
                .filter(String.format("time >= %s AND time <= %s AND surrogate_key = 1 AND family = 'f'", queryFromMillis(), queryToMillis()));
        df = df.select(df.col("time"), df.col("family"), df.col("surrogate_key"), df.col("user_id"), df.col("temperature_k"));

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) df.toJSON().collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37}," +
                "{surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12}," +
                "{surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95}," +
                "{surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121}," +
                "{surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212} " +
                "]", stringify(data));
    }

    @Test
    public void dataFrameReadShouldHandleTimestampAsLong() {
        Dataset<Row> df = sparkSession().read()
                .format("org.apache.spark.sql.riak")
                .option("spark.riakts.bindings.timestamp", "useLong")
                .load(bucketName())
                .filter(String.format("time > %s AND time < %s AND surrogate_key = 1 AND family = 'f'", queryFromMillis(), queryToMillis()));
        df = df.select(df.col("time"), df.col("family"), df.col("surrogate_key"), df.col("user_id"), df.col("temperature_k"));

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) df.toJSON().collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{surrogate_key:1, family: 'f', time: 111111, user_id:'bryce', temperature_k:305.37}," +
                "{surrogate_key:1, family: 'f', time: 111222, user_id:'bryce', temperature_k:300.12}," +
                "{surrogate_key:1, family: 'f', time: 111333, user_id:'bryce', temperature_k:295.95}," +
                "{surrogate_key:1, family: 'f', time: 111444, user_id:'ratman', temperature_k:362.121}," +
                "{surrogate_key:1, family: 'f', time: 111555, user_id:'ratman', temperature_k:3502.212} " +
                "]", stringify(data));
    }
}
