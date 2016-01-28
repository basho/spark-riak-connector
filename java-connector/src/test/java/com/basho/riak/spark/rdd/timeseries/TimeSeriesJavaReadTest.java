package com.basho.riak.spark.rdd.timeseries;

import com.basho.riak.spark.rdd.RiakTSTests;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.collection.Seq;

import java.sql.Timestamp;
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
                .riakTSBucket(bucketName(), Row.class)
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
        SQLContext sqlContext = new SQLContext(jsc);
        JavaRDD<TimeSeriesDataBean> rows = javaFunctions(jsc)
                .riakTSBucket(bucketName(), Row.class)
                .sql(String.format("SELECT time, user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()))
                .map(r -> new TimeSeriesDataBean(r.getTimestamp(0).getTime(), r.getString(1), r.getDouble(2)));

        DataFrame df = sqlContext.createDataFrame(rows, TimeSeriesDataBean.class);
        df.registerTempTable("test");

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) sqlContext.sql("select * from test").toJSON().collect();
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

        SQLContext sqlContext = new SQLContext(jsc);
        JavaRDD<TimeSeriesDataBean> rows = javaFunctions(jsc)
                .riakTSBucket(bucketName(), structType, Row.class)
                .sql(String.format("SELECT time, user_id, temperature_k FROM %s %s", bucketName(), sqlWhereClause()))
                .map(r -> new TimeSeriesDataBean(r.getLong(0), r.getString(1), r.getDouble(2)));

        DataFrame df = sqlContext.createDataFrame(rows, TimeSeriesDataBean.class);
        df.registerTempTable("test");

        // Explicit cast due to compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) sqlContext.sql("select * from test").toJSON().collect();
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
        SQLContext sqlContext = new SQLContext(jsc);

        sqlContext.udf().register("getMillis", (UDF1<Timestamp, Object>) Timestamp::getTime, DataTypes.LongType);

        DataFrame df = sqlContext.read()
                .format("org.apache.spark.sql.riak")
                .schema(schema())
                .load(bucketName())
                .filter(String.format("time > CAST(%s AS TIMESTAMP) AND time < CAST(%s AS TIMESTAMP) AND surrogate_key = 1 AND family = 'f'", queryFrom(), queryTo()));
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
        SQLContext sqlContext = new SQLContext(jsc);

        StructType structType = new StructType(new StructField[]{
                DataTypes.createStructField("surrogate_key", DataTypes.LongType, true),
                DataTypes.createStructField("family", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("temperature_k", DataTypes.DoubleType, true),
        });

        DataFrame df = sqlContext.read()
                .format("org.apache.spark.sql.riak")
                .schema(structType)
                .load(bucketName())
                .filter(String.format("time > %s AND time < %s AND surrogate_key = 1 AND family = 'f'", queryFrom(), queryTo()));
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
