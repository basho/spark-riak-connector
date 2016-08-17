package com.basho.riak.spark.rdd;

import com.basho.riak.spark.japi.SparkJavaUtil;
import com.basho.riak.test.rule.annotations.OverrideRiakClusterConfig;
import com.clearspring.analytics.util.Lists;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import scala.Option;
import scala.Some;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

import static java.lang.Double.compare;
import static java.lang.Long.valueOf;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.Assert.assertEquals;

@OverrideRiakClusterConfig(nodes = 3, timeout = 5)
public class JavaFullBucketReadTest extends AbstractJavaSparkTest {

    private final int NUMBER_OF_TEST_VALUES = 1000;

    @Override
    public Option<String> jsonData() {
        return Some.apply(
                asStrictJSON(rangeClosed(1, NUMBER_OF_TEST_VALUES).mapToObj(i -> new HashMap<String, Object>() {{
                    put("key", "k" + i);
                    put("value", "v" + i);
                    put("indexes", new HashMap<String, String>() {{
                        put("creationNo", String.valueOf(i));
                    }});
                }}).collect(toList()), false)
        );
    }

    @Category(RiakTSTests.class)
    @Test
    public void readAll() {
        List<Tuple2<Integer, Iterable<String>>> data = SparkJavaUtil.javaFunctions(jsc)
                .riakBucket(DEFAULT_NAMESPACE(), String.class)
                .queryAll()
                .mapPartitionsWithIndex(new FuncReMapWithPartitionIdx<>(), true)
                .mapToPair(new FuncMapTupleToJavaPairRdd<>())
                .groupByKey()
                .collect();

        List<String> allValues = data.stream()
                .flatMap(t -> Lists.newArrayList(t._2()).stream())
                .sorted((o1, o2) -> compare(valueOf(o1.substring(1)), valueOf(o2.substring(1))))
                .collect(toList());

        assertEquals(NUMBER_OF_TEST_VALUES, allValues.size());
        rangeClosed(1, NUMBER_OF_TEST_VALUES).forEach(i -> assertEquals("v" + i, allValues.get(i - 1)));
    }
}
