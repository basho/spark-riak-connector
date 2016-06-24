package com.basho.riak.spark.japi.rdd.mapper;

import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.spark.rdd.BucketDef;
import com.basho.riak.spark.rdd.mapper.ReadDataMapper;
import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory;
import com.basho.riak.spark.util.RiakObjectConversionUtil;
import scala.Tuple2;
import scala.Tuple2$;
import scala.reflect.ClassTag;
import scala.reflect.api.TypeTags;

import static com.basho.riak.spark.util.JavaApiHelper.*;

public class DefaultReadDataMapper {

    public static final ReadDataMapperFactory<Tuple2<String, Object>> instance =
            new ReadDataMapperFactory<Tuple2<String, Object>>() {
                @Override
                public Class<Tuple2<String, Object>> targetClass() {
                    return getRuntimeClass(getTypeTag(Tuple2.class, toScalaSeq(new TypeTags.TypeTag<?>[]{
                            getTypeTag(String.class),
                            getTypeTag(Object.class)
                    })));
                }

                @Override
                public ReadDataMapper<Tuple2<String, Object>> dataMapper(BucketDef bucketDef) {
                    return JavaReadDataMapper.instance;
                }
            };


    private static class JavaReadDataMapper implements ReadDataMapper<Tuple2<String, Object>> {
        static final JavaReadDataMapper instance = new JavaReadDataMapper();

        @Override
        public Tuple2<String, Object> mapValue(Location location, RiakObject riakObject, ClassTag<Tuple2<String, Object>> ct) {
            return Tuple2$.MODULE$.apply(
                    location.getKeyAsString(),
                    RiakObjectConversionUtil.from(location, riakObject, getClassTag(Object.class)));
        }
    }
}
