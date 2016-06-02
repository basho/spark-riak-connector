package com.basho.riak.spark.rdd;

import com.basho.riak.spark.japi.rdd.RiakJavaPairRDD;
import com.basho.riak.spark.japi.rdd.RiakJavaRDD;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Option;
import scala.Some;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;

public class RiakJavaRDDQueryRangeTest extends AbstractJavaSparkTest {

    private String CREATION_INDEX = "creationNo";

    @Override
    public Option<String> jsonData() {
        return Some.apply("[" +
                " { key: 'key-1', indexes: {creationNo: 1, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}}" +
                ",{ key: 'key-2', indexes: {creationNo: 2, category: 'visitor'}, value:  {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}}" +
                ",{ key: 'key-3', indexes: {creationNo: 3, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}}" +
                ",{ key: 'key-4', indexes: {creationNo: 4, category: 'stranger'}, value: {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}}" +
                ",{ key: 'key-5', indexes: {creationNo: 5, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}}" +
                ",{ key: 'key-6', indexes: {creationNo: 6, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}}" +
                ",{ key: 'key-7', indexes: {creationNo: 7, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}}" +
                "]");
    }

    @Category(RiakCommonTests.class)
    @Test
    public void check2iRangeQuery() {
        RiakJavaRDD<Map> rdd = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), Map.class)
                .query2iRange(CREATION_INDEX, 2L, 4L);
        List<Map> results = rdd.collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                        " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
                        ",{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
                        ",{user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
                        "]",
                results);
    }

    @Category(RiakCommonTests.class)
    @Test
    public void check2iRangeQueryPairRDD() {
        RiakJavaPairRDD<String, Map> rdd = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), String.class, Map.class)
                .query2iRange(CREATION_INDEX, 2L, 4L);
        List<Tuple2<String, Map>> results = rdd.collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                        " ['key-2', {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}]" +
                        ",['key-3', {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}]" +
                        ",['key-4', {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}]" +
                        "]",
                results);
    }

    @Category(RiakCommonTests.class)
    @Test
    public void check2iRangeLocalQuery() {
        RiakJavaRDD<Map> rdd = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), Map.class)
                .query2iRangeLocal(CREATION_INDEX, 2L, 4L);
        List<Map> results = rdd.collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                        " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
                        ",{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
                        ",{user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
                        "]",
                results);
    }

    @Test
    @Category(RiakCommonTests.class)
    public void check2iPartitionByIntegerKeyRanges() {
        List<Tuple2<Integer, Iterable<UserTS>>> data = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), UserTS.class)
                .partitionBy2iRanges(CREATION_INDEX, new Tuple2<>(1, 3), new Tuple2<>(4, 6), new Tuple2<>(7, 12))
                .mapPartitionsWithIndex(new FuncReMapWithPartitionIdx<>(), true)
                .mapToPair(new FuncMapTupleToJavaPairRdd<>()) // TODO: investigate better way to get instance of JavaPairRDD
                .groupByKey()
                .collect();

        assertEqualsUsingJSONIgnoreOrder("[" +
                        // The 1st partition should contains first 3 item
                        "[0, [" +
                        "     {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}" +
                        "     ,{user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
                        "     ,{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
                        "]]" +

                        // The 2nd partition should contsins items; 4,5,6
                        ",[1, [" +
                        "     {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
                        "     ,{user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}" +
                        "     ,{user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}" +
                        "]]" +

                        // The 3rd partition should contains the only 7th item
                        ",[2, [" +
                        "     {user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}" +
                        "]]" +
                        "]",
                data);
    }
}
