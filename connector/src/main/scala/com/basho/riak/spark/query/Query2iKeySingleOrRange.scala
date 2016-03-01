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

import com.basho.riak.client.api.commands.indexes.{BigIntIndexQuery, BinIndexQuery, IntIndexQuery}
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{Location, Namespace}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.connector.RiakSession
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}

import scala.collection.JavaConversions._

private case class Query2iKeySingleOrRange[K](bucket: BucketDef, readConf: ReadConf, index: String, from: K,
                                              to: Option[K] = None, coverageEntries: Option[Seq[CoverageEntry]] = None)
    extends LocationQuery[Either[String, CoverageEntry]] {

  private val iter = coverageEntries match {
    case None      => None
    case Some(seq) => Some(seq.iterator)
  }
  private var ce: Option[CoverageEntry] = None

  private def isSuitableForIntIndex(v: K): Boolean = v match {
    case _: Long => true
    case _: Int  => true
    case _       => false
  }

  private def isSuitableForBigIntIndex(v: K): Boolean = v match {
    case _: BigInt     => true
    case _: BigInteger => true
    case _             => false
  }

  private def convertToLong(value: K): Long = value match {
    case i: Int  => i.toLong
    case l: Long => l
  }

  private def convertToBigInteger(value: K): BigInteger = value match {
    case i: BigInt      => i.underlying()
    case bi: BigInteger => bi
  }

  // This method is looks ugly, but to fix that we need to introduce changes in Riak Java Client
  // scalastyle:off cyclomatic.complexity method.length
  override def nextLocationChunk(nextToken: Option[_], session: RiakSession): (Option[Either[String, CoverageEntry]], Iterable[Location]) = {
    val ns = new Namespace(bucket.bucketType, bucket.bucketName)

    ce = nextToken match {
      case None => iter match {
        case None           => None
        case Some(iterator) => Some(iterator.next)
      }

      case Some(Left(_: String))                     => ce

      case Some(Right(coverageEntry: CoverageEntry)) => Some(coverageEntry)

      case _ =>
        throw new IllegalArgumentException("Invalid nextToken")
    }

    val builder = from match {

      case coverageEntry: CoverageEntry =>
        // Full Bucket Read (Query all data)

        require(to.isEmpty, "Coverage Entry can't be used in a range manner, therefore 'to' parameter must be None")
        require(coverageEntries.isEmpty, "The Coverage Entry parameter mustn't be used for this type of query")

        new BinIndexQuery.Builder(ns, index, coverageEntry.getCoverageContext)

      case _ if isSuitableForIntIndex(from) => to match {
        case None    => new IntIndexQuery.Builder(ns, index, convertToLong(from))
        case Some(v) => new IntIndexQuery.Builder(ns, index, convertToLong(from), convertToLong(v))
      }

      case _ if isSuitableForBigIntIndex(from) => to match {
        case None    => new BigIntIndexQuery.Builder(ns, index, convertToBigInteger(from))
        case Some(v) => new BigIntIndexQuery.Builder(ns, index, convertToBigInteger(from), convertToBigInteger(v))
      }

      case str: String => to match {
        case None            => new BinIndexQuery.Builder(ns, index, str)
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

    ce match {
      case None                               =>
      case Some(coverageEntry: CoverageEntry) => builder.withCoverageContext(coverageEntry.getCoverageContext) // local 2i query (coverage entry is provided) either Equal or Range
    }

    nextToken match {
      case Some(Left(continuation: String)) => builder.withContinuation(BinaryValue.create(continuation))
      case _                                =>
    }

    val request = builder match {
      case iQueryBuilder: IntIndexQuery.Builder       => iQueryBuilder.build()
      case bigIQueryBuilder: BigIntIndexQuery.Builder => bigIQueryBuilder.build()
      case bQueryBuilder: BinIndexQuery.Builder       => bQueryBuilder.build()
    }

    val response = session.execute(request)

    // gathering locations, if any
    var locations: Iterable[Location] = Nil

    val resposeEntries = response match {
      case iQueryResponse: IntIndexQuery.Response       => iQueryResponse.getEntries
      case bigIQueryResponse: BigIntIndexQuery.Response => bigIQueryResponse.getEntries
      case bQueryResponse: BinIndexQuery.Response       => bQueryResponse.getEntries
    }
    resposeEntries.toSeq match {
      case Nil if iter.isDefined && iter.get.hasNext => nextLocationChunk(None, session)
      case _ =>
        locations = resposeEntries.map(_.getRiakObjectLocation)

        if (!response.hasContinuation) {
          iter match {
            case Some(iterator) if iterator.hasNext => Some(Right(iterator.next)) -> locations
            case _ => None -> locations
          }
        } else {
          Some(Left(response.getContinuation.toStringUtf8)) -> locations
        }
    }
    // scalastyle:on null
  }
  // scalastyle:on cyclomatic.complexity method.length
}
