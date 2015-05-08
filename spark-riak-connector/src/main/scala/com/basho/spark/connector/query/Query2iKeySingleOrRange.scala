package com.basho.spark.connector.query


import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.indexes.{BinIndexQuery, IntIndexQuery}
import com.basho.riak.client.core.query.{Namespace, Location}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.spark.connector.rdd.{ReadConf, BucketDef}

import scala.collection.JavaConversions._

case class Query2iKeySingleOrRange[K](bucket: BucketDef, readConf: ReadConf, index: String, from: K, to: Option[K] = None )
    extends Query[String] {

  private def isSuitableForIntIndex(from:K): Boolean = from match {
    case _: Long => true
    case _: Int => true
    case _ => false
  }

  private def convertToLong(value: K): Long = value match {
    case i: Int => i.toLong
    case l: Long => l
  }

  override def nextLocationBulk(nextToken: Option[_], session: RiakClient): (Option[String], Iterable[Location]) = {
    val ns = new Namespace(bucket.bucketType, bucket.bucketName)
    val builder = from match {
      case _ if isSuitableForIntIndex(from) => to match {
          case None => new IntIndexQuery.Builder(ns, index, convertToLong(from))
          case Some(v) =>  new IntIndexQuery.Builder(ns, index, convertToLong(from), convertToLong(v))
        }

      case str: String => to match {
          case None => new BinIndexQuery.Builder(ns, index, str)
          case Some(v: String) => new BinIndexQuery.Builder(ns, index, str)
        }

      case _ =>
        throw new IllegalArgumentException("Unsupported 2i key type")
    }

    builder
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)

    nextToken match {
      case None =>
        /* It is a first request */

      case Some(continuation: String) =>
        /* Subsequent request */
        builder.withContinuation(BinaryValue.create(continuation))

      case _ =>
        throw new IllegalArgumentException("Wrong nextToken")
    }

    val request = builder match {
      case iQueryBuilder: IntIndexQuery.Builder => iQueryBuilder.build()
      case bQueryBuilder: BinIndexQuery.Builder => bQueryBuilder.build()
    }

    val response = session.execute(request)

    // gathering locations, if any
    var locations: Iterable[Location] = Nil

    if( response.hasEntries){
      val entries = response match {
        case iQueryResponse: IntIndexQuery.Response => iQueryResponse.getEntries
        case bQueryResponse: BinIndexQuery.Response => bQueryResponse.getEntries
      }
      locations = entries.map(_.getRiakObjectLocation)
    }

    if(response.getContinuation == null){
      None -> locations
    }else{
      Some(response.getContinuation.toStringUtf8) -> locations
    }
  }
}