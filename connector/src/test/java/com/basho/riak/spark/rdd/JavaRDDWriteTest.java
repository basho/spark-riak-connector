package com.basho.riak.spark.rdd;

import static com.basho.riak.spark.japi.SparkJavaUtil.javaFunctions;
import static org.junit.Assert.assertEquals;
import static scala.collection.JavaConversions.seqAsJavaList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import org.junit.experimental.categories.Category;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;

import com.basho.riak.client.api.annotations.RiakIndex;
import com.basho.riak.client.api.annotations.RiakKey;
import com.basho.riak.spark.writer.mapper.TupleWriteDataMapper;

@Category(RiakCommonTests.class)
public class JavaRDDWriteTest extends AbstractJavaSparkTest {

    @Test
    public void saveToRiakWithORM() {
        List<ORMDomainObject> objs = Arrays.asList(
                new ORMDomainObject("user-1", 1, "user1"),
                new ORMDomainObject("user-2", 2, "user2"),
                new ORMDomainObject("user-3", 3, "user3"));
        JavaRDD<ORMDomainObject> rdd = jsc.parallelize(objs);
        javaFunctions(rdd).saveToRiak(DEFAULT_NAMESPACE_4STORE());
        List<ORMDomainObject> data = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE_4STORE(), ORMDomainObject.class)
                .queryBucketKeys("user-1", "user-2", "user-3")
                .collect();
        assertEqualsUsingJSONIgnoreOrder("[" +
                "{login:'user1', group_id:'${json-unit.ignore}', userId:'${json-unit.ignore}'}," +
                "{login:'user2', group_id:'${json-unit.ignore}', userId:'${json-unit.ignore}'}," +
                "{login:'user3', group_id:'${json-unit.ignore}', userId:'${json-unit.ignore}'}" +
                "]", data);
    }

    @Test
    public void saveToRiakTuple1() {
        JavaRDD<Tuple1> rdd = jsc.parallelize(Arrays.asList(
                new Tuple1<>("key1"), new Tuple1<>("key2"), new Tuple1<>("key3")
        ), 1);

        javaFunctions(rdd).saveToRiak(DEFAULT_NAMESPACE_4STORE(), TupleWriteDataMapper.factory());
        
        List<Tuple2<String, String>> t1Data = seqAsJavaList(fetchAllFromBucket(DEFAULT_NAMESPACE_4STORE()));

        assertEqualsUsingJSONIgnoreOrder("[" +
                "['${json-unit.ignore}', 'key1']," +
                "['${json-unit.ignore}', 'key2']," +
                "['${json-unit.ignore}', 'key3']" +
                "]", t1Data);
    }

    @Test
    public void saveToRiakTuple2() {
        JavaRDD<Tuple2> rdd = jsc.parallelize(Arrays.asList(
                new Tuple2<>("key1", 1), new Tuple2<>("key2", 2), new Tuple2<>("key3", 3)
        ), 1);

        javaFunctions(rdd).saveToRiak(DEFAULT_NAMESPACE_4STORE(), TupleWriteDataMapper.factory());

        List<Integer> data = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE_4STORE(), Integer.class)
                .queryBucketKeys("key1", "key2", "key3")
                .collect();

        assertEquals(3, data.size());
        assertEqualsUsingJSONIgnoreOrder("[1,2,3]", data);
    }

    @Test
    public void saveToRiakTuple3() {
        JavaRDD<Tuple3> rdd = jsc.parallelize(Arrays.asList(
                new Tuple3<>("key1", 1, 11), new Tuple3<>("key2", 2, 22), new Tuple3<>("key3", 3, 33)
        ), 1);

        javaFunctions(rdd).saveToRiak(DEFAULT_NAMESPACE_4STORE(), TupleWriteDataMapper.factory());

        List<List> data = javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE_4STORE(), List.class)
                .queryBucketKeys("key1", "key2", "key3")
                .collect();

        assertEqualsUsingJSONIgnoreOrder("[" +
                "[1,11]," +
                "[2,22]," +
                "[3,33]" +
                "]", data);
    }

    @SuppressWarnings("unused")
    private static class ORMDomainObject implements Serializable {
        @RiakKey
        private String userId;

        @RiakIndex(name = "groupId")
        private long group_id;

        private String login;

        public ORMDomainObject() {
        }

        public ORMDomainObject(String userId, long group_id, String login) {
            this.userId = userId;
            this.group_id = group_id;
            this.login = login;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public long getGroup_id() {
            return group_id;
        }

        public void setGroup_id(long group_id) {
            this.group_id = group_id;
        }

        public String getLogin() {
            return login;
        }

        public void setLogin(String login) {
            this.login = login;
        }
    }
}
