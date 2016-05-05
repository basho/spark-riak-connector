package com.basho.riak.spark.rdd.mapper

import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.util.RiakObjectConversionUtil

import scala.reflect.ClassTag

class ReadValueDataMapper[T] extends ReadDataMapper[T] {
  override def mapValue(location: Location, riakObject: RiakObject)(implicit ct: ClassTag[T]): T =
    RiakObjectConversionUtil.from[T](location, riakObject)
}

object ReadValueDataMapper {
  def factory[T](implicit ct: ClassTag[T]): ReadDataMapperFactory[T] = new ReadDataMapperFactory[T] {
    override def dataMapper(bucketDef: BucketDef) = new ReadValueDataMapper[T]
    override def targetClass: Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
  }
}