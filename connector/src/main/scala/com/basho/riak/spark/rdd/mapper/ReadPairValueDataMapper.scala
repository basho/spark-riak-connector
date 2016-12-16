package com.basho.riak.spark.rdd.mapper

import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.spark.rdd.BucketDef
import com.basho.riak.spark.util.RiakObjectConversionUtil

import scala.reflect.ClassTag

class ReadPairValueDataMapper[K, V](implicit
                                    kCt: ClassTag[K],
                                    vCt: ClassTag[V]
                                   ) extends ReadDataMapper[(K, V)] {
  override def mapValue(location: Location, riakObject: RiakObject)(implicit ct: ClassTag[(K, V)]): (K, V) = {
    // The only String keys are supported for now
    val r = location.getKeyAsString.asInstanceOf[K] -> ReadValueDataMapper.mapValue[V](location,riakObject)
    r.asInstanceOf[(K,V)]
  }
}

object ReadPairValueDataMapper {
  def factory[K, V](implicit
                    kCt: ClassTag[K],
                    vCt: ClassTag[V]
                   ): ReadDataMapperFactory[(K, V)] = new ReadDataMapperFactory[(K, V)] {
    override def dataMapper(bucketDef: BucketDef): ReadDataMapper[(K, V)] = new ReadPairValueDataMapper[K, V]

    override def targetClass: Class[(K, V)] = classOf[(K, V)]
  }
}
