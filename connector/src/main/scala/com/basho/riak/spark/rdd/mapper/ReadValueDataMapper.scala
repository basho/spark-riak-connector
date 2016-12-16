package com.basho.riak.spark.rdd.mapper

import com.basho.riak.client.api.convert.ConverterFactory
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.rdd.BucketDef

import scala.reflect.ClassTag

class ReadValueDataMapper[T] extends ReadDataMapper[T] {
  override def mapValue(location: Location, riakObject: RiakObject)(implicit ct: ClassTag[T]): T =
    ReadValueDataMapper.mapValue[T](location, riakObject)
}

object ReadValueDataMapper {
  def factory[T](implicit ct: ClassTag[T]): ReadDataMapperFactory[T] = new ReadDataMapperFactory[T] {
    override def dataMapper(bucketDef: BucketDef) = new ReadValueDataMapper[T]
    override def targetClass: Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
  }

  def mapValue[T](location: Location, riakObject: RiakObject)(implicit ct: ClassTag[T]): T =
    (ct.runtimeClass match {
      // It's necessary to identify cases when parameter type is not specified (when T is Any)
      case x: Class[_] if x == classOf[Any] => parseContentTypeAndCharset(riakObject.getContentType) match {
        case ("text/plain", _) => ConverterFactory.getInstance.getConverter(classOf[String])
        case ("application/json", _) => ConverterFactory.getInstance.getConverter(classOf[Any])
        case _ => throw new IllegalStateException("Data type cannot be inferred by RiakObject content type.")
      }
      case x: Class[_] => ConverterFactory.getInstance.getConverter(x)
    }).toDomain(riakObject, location)

  private def parseContentTypeAndCharset(contentType: String): (String, String) =
    contentType.split(";").map(x => x.trim.toLowerCase).toList match {
      case ct :: others => ct -> others.find(x => x.startsWith("charset")).getOrElse("UTF-8")
      case Nil => throw new IllegalArgumentException(s"Content type value '$contentType' cannot be parsed")
    }

}