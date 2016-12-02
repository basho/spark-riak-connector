package com.basho.riak.spark.japi.rdd.mapper;

import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory;
import com.basho.riak.spark.rdd.mapper.ReadPairValueDataMapper;
import scala.Tuple2;

import static com.basho.riak.spark.util.JavaApiHelper.*;

public class DefaultReadDataMapper {
    public static final ReadDataMapperFactory<Tuple2<String, Object>> instance = ReadPairValueDataMapper.factory(getClassTag(String.class), getClassTag(Object.class));
}
