package com.basho.spark.connector.rdd

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.{RiakObject, Location}
import com.basho.spark.connector.util.{RiakObjectConversionUtil, Logging}

import scala.reflect.ClassTag


abstract class AbstractRDDTest extends AbstractRiakSparkTest with Logging{

  protected def readByLocation[T:ClassTag](riakSession: RiakClient, location: Location): T =
    readByLocation(riakSession, location, (l: Location, ro: RiakObject)  => {RiakObjectConversionUtil.from[T](l, ro)})
}
