package com.basho.riak.spark.japi;

import java.io.Serializable;

import com.basho.riak.spark.rdd.connector.RiakConnector;
import com.basho.riak.spark.rdd.connector.RiakConnector$;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.spark.rdd.RDDFunctions;
import com.basho.riak.spark.util.JavaApiHelper;
import com.basho.riak.spark.writer.RiakWriter;
import com.basho.riak.spark.writer.WriteConf;
import com.basho.riak.spark.writer.WriteDataMapperFactory;
import com.basho.riak.spark.writer.WriteDataMapperFactory$;
import com.basho.riak.spark.writer.mapper.DefaultWriteDataMapper;
import com.basho.riak.spark.writer.ts.RowDef;

public class RDDJavaFunctions<T> {

    public final RDD<T> rdd;
    public final RDDFunctions<T> rddFunctions;

    public RDDJavaFunctions(RDD<T> rdd) {
        this.rdd = rdd;
        this.rddFunctions = new RDDFunctions<>(rdd);
    }

    public RiakConnector defaultConnector() {
        return RiakConnector$.MODULE$.apply(rdd.conf());
    }

    public SparkConf getConf() {
        return rdd.conf();
    }
    
    public void saveToRiak(String bucketName, String bucketType) {
      new RDDFunctions<>(rdd).saveToRiak(bucketName, bucketType, 
          WriteConf.apply(getConf()), RiakConnector.apply(getConf()),
          DefaultWriteDataMapper.factory());
    }
      
    public void saveToRiak(String bucketName) {
      saveToRiak(bucketName, "default");
    }
    
    public void saveToRiak(Namespace ns) {
      saveToRiak(ns.getBucketNameAsString(), ns.getBucketTypeAsString());
    }

    public void saveToRiak(
            String bucketName,
            String bucketType,
            WriteConf writeConf,
            RiakConnector connector,
            WriteDataMapperFactory<T, Tuple2<String, Object>> vwf
    ) {
        new RDDFunctions<>(rdd).saveToRiak(bucketName, bucketType, writeConf, connector, vwf);
    }

    public void saveToRiak(
            String bucketName,
            WriteDataMapperFactory<T, Tuple2<String, Object>> vwf
    ) {
        new RDDFunctions<>(rdd).saveToRiak(bucketName, "default",
                WriteConf.apply(getConf()), RiakConnector.apply(getConf()), vwf);
    }

    public void saveToRiak(
            Namespace ns,
            WriteDataMapperFactory<T, Tuple2<String, Object>> vwf
    ) {
        new RDDFunctions<>(rdd).saveToRiak(ns, vwf);
    }

    public void saveToRiakTS(
            RiakConnector connector,
            String bucketType,
            String bucketName,
            WriteConf writeConf,
            WriteDataMapperFactory<T, RowDef> factory
    ) {
        RiakWriter<T, RowDef> writer = RiakWriter.tsWriter(connector, bucketType, bucketName, writeConf, factory);
        rdd.sparkContext().runJob(rdd, new JobFunc<>(writer), JavaApiHelper.getClassTag(Void.class));
    }

    @SuppressWarnings("unchecked")
    public void saveToRiakTS(String bucketName) {
        final WriteDataMapperFactory<T, RowDef> factory = (WriteDataMapperFactory<T, RowDef>) WriteDataMapperFactory$.MODULE$.sqlRowFactory();
        final String bucketTypeStr ="default";
        saveToRiakTS(RiakConnector.apply(getConf()), bucketTypeStr, bucketName,
                WriteConf.apply(getConf()), factory);
    }

    public static class JobFunc<T, U> extends AbstractFunction1<Iterator<T>, Void> implements Serializable {
        private final RiakWriter<T, U> writer;

        public JobFunc(RiakWriter<T, U> writer) {
            this.writer = writer;
        }

        @Override
        public Void apply(Iterator<T> iterator) {
            writer.write(TaskContext.get(), iterator);
            return null;
        }
    }
}
