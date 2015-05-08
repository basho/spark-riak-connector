package com.basho.spark

import com.basho.spark.connector.rdd.RDDFunctions
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect.ClassTag

package object connector {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)

  implicit def toRDDFunctions[T : ClassTag](rdd: RDD[T]): RDDFunctions[T] =
    new RDDFunctions[T](rdd)

}
