package com.basho.riak.spark.rdd;

import com.basho.riak.spark.japi.SparkJavaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Option;
import scala.Some;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;

public class RiakJavaRDDQueryKeysTest extends AbstractJavaSparkTest {

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
    public void checkBucketKeysQuery() {
        List<Map> results = SparkJavaUtil.javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), Map.class)
                .queryBucketKeys("key-1", "key-4", "key-6").collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                        "{timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}," +
                        "{timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}," +
                        "{timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}" +
                        "]",
                results);
    }

    @Category(RiakCommonTests.class)
    @Test
    public void check2iRangeQueryPairRDD() {
        List<Tuple2<String, Map>> results = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), String.class, Map.class)
                .queryBucketKeys("key-1", "key-4", "key-6")
                .collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                        "  ['key-1', {timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}]" +
                        ", ['key-4', {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}]" +
                        ", ['key-6', {timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}]" +
                        "]",
                results);
    }

    @Category(RiakCommonTests.class)
    @Test
    public void check2iKeysQuery() {
        List<Map> results = SparkJavaUtil.javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), Map.class)
                .query2iKeys("category", "stranger", "visitor").collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                        "{timestamp: '2014-11-24T13:15:04.824Z', user_id: 'u1'}" +
                        ", {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}" +
                        ", {timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}" +
                        ", {timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}" +
                        ", {timestamp: '2014-11-24T12:01:04.825Z', user_id: 'u3'}" +
                        "]",
                results);
    }

    @Category(RiakCommonTests.class)
    @Test
    public void check2iPartitionByKeys() {
        List<Tuple2<Integer, Iterable<UserTS>>> data = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), UserTS.class)
                .partitionBy2iKeys("category", "neighbor", "visitor", "stranger")
                .mapPartitionsWithIndex(new FuncReMapWithPartitionIdx<>(), true)
                .mapToPair(new FuncMapTupleToJavaPairRdd<>()) // TODO: investigate better way to get instance of JavaPairRDD
                .groupByKey()
                .collect();

        assertEqualsUsingJSONIgnoreOrder(
                "[" +

                        // 1st partition should contains 2 neighbors
                        "[0, [" +
                        " {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}" +
                        " ,{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
                        "]]" +

                        // 2nd partition should contains 1 visitor
                        ",[1, [" +
                        " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
                        "]]" +

                        // 3rd partition should contains 4 strangers
                        ",[2, [" +
                        " {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
                        " ,{user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}" +
                        " ,{user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}" +
                        " ,{user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}" +
                        "]]" +
                        "]",
                data
        );
    }
}
