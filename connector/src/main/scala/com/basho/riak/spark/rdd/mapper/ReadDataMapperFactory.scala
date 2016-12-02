package com.basho.riak.spark.rdd.mapper

import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.util.DataMapper

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait ReadDataMapper[T] extends DataMapper {
  def mapValue(location: Location, riakObject: RiakObject)(implicit ct: ClassTag[T]): T
}

trait ReadDataMapperFactory[T] extends Serializable {
  def dataMapper(bucketDef: BucketDef): ReadDataMapper[T]
  def targetClass: Class[T]
}

/**
  * Allows using data mapper objects as a factory
  */
trait ReadDataMapperAsFactory[T] extends ReadDataMapperFactory[T] {
  this: ReadDataMapper[T] =>
  override def dataMapper(bucketDef: BucketDef): ReadDataMapper[T] = this
}

trait LowPriorityReadDataMapperFactoryImplicits {

  // scalastyle:off null
  trait IsNotSubclassOf[A, B]
  implicit def nsub[A, B]: A IsNotSubclassOf B = null
  implicit def nsubAmbiguity1[A, B >: A]: A IsNotSubclassOf B = null
  implicit def nsubAmbiguity2[A, B >: A]: A IsNotSubclassOf B = null
  // scalastyle:on null

  implicit def singleValueReaderFactory[T: ClassTag](implicit
                                                     ev1: T IsNotSubclassOf (_, _),
                                                     ev2: T IsNotSubclassOf (_, _, _)
                                                    ): ReadDataMapperFactory[T] = ReadValueDataMapper.factory

  // K =:= String is used only String keys are supported for now
  implicit def pairValueReaderFactory[K: ClassTag, V: ClassTag](implicit ev: K =:= String
                                                               ): ReadDataMapperFactory[(K, V)] = ReadPairValueDataMapper.factory
}

object ReadDataMapperFactory extends LowPriorityReadDataMapperFactoryImplicits {

  // Any is using because RDD type will be inferred by RiakObject content type
  implicit object DefaultReadDataMapper extends ReadDataMapper[(String, Any)]
    with ReadDataMapperAsFactory[(String, Any)] {

    override def mapValue(location: Location,
                          riakObject: RiakObject
                         )(implicit
                           ct: ClassTag[(String, Any)]
                         ): (String, Any) =
      location.getKeyAsString -> ReadValueDataMapper.mapValue[Any](location, riakObject)

    override def targetClass: Class[(String, Any)] = classOf[(String, Any)]
  }

}
