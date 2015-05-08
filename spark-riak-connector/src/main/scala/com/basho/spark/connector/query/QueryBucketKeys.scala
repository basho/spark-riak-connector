package com.basho.spark.connector.query

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.Location
import com.basho.spark.connector.rdd.{BucketDef, ReadConf}
import com.basho.spark.connector.util.Logging

import scala.collection.mutable.ArrayBuffer

case class QueryBucketKeys(bucket: BucketDef, readConf:ReadConf, keys: Iterable[String]) extends QuerySubsetOfKeys[String] with Logging{
  override def locationsByKeys(keys: Iterator[String], session: RiakClient): Iterable[Location] = {
    val dataBuffer = new ArrayBuffer[Location](readConf.fetchSize)

    val ns = bucket.asNamespace()

    keys.exists(k =>{
      dataBuffer += new Location(ns, k)
      dataBuffer.size < readConf.fetchSize} )
    dataBuffer
  }
}