/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.riak.spark.query

import java.math.BigInteger

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.indexes.{BigIntIndexQuery, BinIndexQuery, IntIndexQuery}
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{Namespace, Location}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.{ReadConf, BucketDef}

import scala.collection.JavaConversions._

private case class Query2iKeySingleOrRange[K](bucket: BucketDef, readConf: ReadConf, index: String, from: K,
        to: Option[K] = None, coverageEntry: Option[CoverageEntry] = None )
    extends Query[String] {

  private def isSuitableForIntIndex(v:K): Boolean = v match {
    case _: Long => true
    case _: Int => true
    case _ => false
  }

  private def isSuitableForBigIntIndex(v: K): Boolean = v match {
    case _: BigInt => true
    case _: BigInteger => true
    case _ => false
  }

  private def convertToLong(value: K): Long = value match {
    case i: Int => i.toLong
    case l: Long => l
  }

  private def convertToBigInteger(value: K): BigInteger = value match {
    case i: BigInt => i.underlying()
    case bi:BigInteger => bi
  }

  // This method is looks ugly, but to fix that we need to introduce changes in Riak Java Client
    // scalastyle:off cyclomatic.complexity method.length
  override def nextLocationBulk(nextToken: Option[_], session: RiakClient): (Option[String], Iterable[Location]) = {
    val ns = new Namespace(bucket.bucketType, bucket.bucketName)
    val builder = from match {

      case ce: CoverageEntry =>
        // Full Bucket Read (Query all data)

        require(to.isEmpty, "Coverage Entry can't be used in a range manner, therefore 'to' parameter must be None")
        require(coverageEntry.isEmpty, "The Coverage Entry parameter mustn't be used for this type of query")

        new BinIndexQuery.Builder(ns, index, ce.getCoverageContext)

      case _ if isSuitableForIntIndex(from) => to match {
          case None => new IntIndexQuery.Builder(ns, index, convertToLong(from))
          case Some(v) =>  new IntIndexQuery.Builder(ns, index, convertToLong(from), convertToLong(v))
        }

      case _ if isSuitableForBigIntIndex(from) => to match {
        case None => new BigIntIndexQuery.Builder(ns, index, convertToBigInteger(from))
        case Some(v) =>  new BigIntIndexQuery.Builder(ns, index, convertToBigInteger(from), convertToBigInteger(v))
      }

      case str: String => to match {
          case None => new BinIndexQuery.Builder(ns, index, str)
          case Some(v: String) => new BinIndexQuery.Builder(ns, index, str)

          case _ =>
            throw new IllegalArgumentException("Illegal 2i end range value")
        }

      case _ =>
        throw new IllegalArgumentException("Unsupported 2i key type")
    }

    builder
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)

    if(coverageEntry.isDefined){
      // local 2i query (coverage entry is provided) either Equal or Range
      builder.withCoverageContext(coverageEntry.get.getCoverageContext)
    }

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
      case bigIQueryBuilder: BigIntIndexQuery.Builder => bigIQueryBuilder.build()
      case bQueryBuilder: BinIndexQuery.Builder => bQueryBuilder.build()
    }

    val response = session.execute(request)

    // gathering locations, if any
    var locations: Iterable[Location] = Nil

    if( response.hasEntries){
      val entries = response match {
        case iQueryResponse: IntIndexQuery.Response => iQueryResponse.getEntries
        case bigIQueryResponse: BigIntIndexQuery.Response => bigIQueryResponse.getEntries
        case bQueryResponse: BinIndexQuery.Response => bQueryResponse.getEntries
      }
      locations = entries.map(_.getRiakObjectLocation)
    }

    // scalastyle:off null
    if(response.getContinuation == null){
      None -> locations
    }else{
      Some(response.getContinuation.toStringUtf8) -> locations
    }
    // scalastyle:on null
  }
  // scalastyle:on cyclomatic.complexity method.length
}
