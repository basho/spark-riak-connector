package com.basho.spark.connector.japi;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public final class SparkJavaUtil {

    private SparkJavaUtil() {
    }

    public static SparkContextJavaFunctions javaFunctions(SparkContext sparkContext) {
        return new SparkContextJavaFunctions(sparkContext);
    }

    public static SparkContextJavaFunctions javaFunctions(JavaSparkContext sparkContext) {
        return new SparkContextJavaFunctions(JavaSparkContext.toSparkContext(sparkContext));
    }
}
