package com.basho.riak.spark.rdd.timeseries;

import com.basho.riak.spark.rdd.RiakTSTests;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.collection.Seq;

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
                .map(r -> new TimeSeriesDataBean(r.getLong(0), r.getString(1), r.getDouble(2)));

        DataFrame df = sqlContext.createDataFrame(rows, TimeSeriesDataBean.class);
        df.registerTempTable("test");

        // Compilation error "Object cannot be converted to java.lang.String[]"
        String[] data = (String[]) sqlContext.sql("select * from test").toJSON().collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{time:111111, user_id:'bryce', temperature_k:305.37}," +
                "{time:111222, user_id:'bryce', temperature_k:300.12}," +
                "{time:111333, user_id:'bryce', temperature_k:295.95}," +
                "{time:111444, user_id:'ratman',temperature_k:362.121}," +
                "{time:111555, user_id:'ratman',temperature_k:3502.212}" +
                "]", stringify(data));
    }
}
