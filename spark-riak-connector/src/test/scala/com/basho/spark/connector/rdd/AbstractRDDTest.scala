package com.basho.spark.connector.rdd

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.{Namespace, RiakObject, Location}
import com.basho.spark.connector.util.RiakObjectConversionUtil
import org.apache.spark.Logging

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


abstract class AbstractRDDTest extends AbstractRiakSparkTest with Logging{

  protected def fetchAllFromBucket(ns:Namespace): List[(String,String)] = {
    val data =  ListBuffer[(String,String)]()
    withRiakDo(session=>
      foreachKeyInBucket(session, ns, (client, l: Location) =>{
        val v = readByLocation[String](client, l)
        data += ((l.getKeyAsString,v))
        false
      })
    )
    data.toList
  }

  protected def readByLocation[T:ClassTag](riakSession: RiakClient, location: Location): T =
    readByLocation(riakSession, location, (l: Location, ro: RiakObject)  => {RiakObjectConversionUtil.from[T](l, ro)})
}
