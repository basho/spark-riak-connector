package com.basho.riak.spark.query

import com.basho.riak.client.api.commands.kv.FullBucketRead
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.connector.RiakSession
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}

import scala.collection.JavaConversions._

case class QueryFullBucket(bucket: BucketDef,
                           readConf: ReadConf,
                           coverageEntries: Iterable[CoverageEntry],
                           index: Option[String] = None
                          ) extends LocationQuery[Either[String, CoverageEntry]] {

  private val coverageEntriesIt = coverageEntries.iterator

  private var coverageEntry: Option[CoverageEntry] = None
  private var query2iKey: Option[Query2iKeySingleOrRange[CoverageEntry]] = None

  override def nextChunk(nextToken: Option[_],
                         session: RiakSession): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
    coverageEntry = nextToken match {
      case None => Some(coverageEntriesIt.next()) // First call, next token is not defined yet
      case Some(Left(_)) => coverageEntry // Recursive call occurred. We still querying same CE
      case Some(Right(ce: CoverageEntry)) => Some(ce) // Next CE was provided
      case _ => throw new IllegalArgumentException("Invalid nextToken")
    }

    if (readConf.useStreamingValuesForFBRead) {
      // Direct fetch is using for BDP version where FullBucketRead query is supported
      oneStepFetch(nextToken.asInstanceOf[Option[Either[String, CoverageEntry]]], session)
    } else {
      /* Indirect fetch is using for TS/KV Riak version. Data will be received using sequence of queries:
         Query2iKeySingleOrRange - for fetching locations and MultiFetch for receiving values by known locations */
      twoStepFetch(nextToken.asInstanceOf[Option[Either[String, CoverageEntry]]], session)
    }
  }

  private def oneStepFetch(nextToken: Option[Either[String, CoverageEntry]],
                          session: RiakSession): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
    val builder = new FullBucketRead.Builder(bucket.asNamespace(), coverageEntry.get.getCoverageContext)
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)
      .withReturnBody(true)

    nextToken.foreach {
      case Left(continuation: String) => builder.withContinuation(BinaryValue.create(continuation))
      case _ =>
    }

    val response: FullBucketRead.Response = session.execute(builder.build())
    response.getEntries.toList match {
      case Nil if coverageEntriesIt.hasNext => nextChunk(None, session)
      case entries: List[FullBucketRead.Response.Entry] =>
        val data = entries.map(e => e.getLocation -> e.getFetchedValue.getValue(classOf[RiakObject]))
        response.hasContinuation match {
          case true => Some(Left(response.getContinuation.toStringUtf8)) -> data
          case _ if coverageEntriesIt.hasNext => Some(Right(coverageEntriesIt.next())) -> data
          case _ => None -> data
        }
    }
  }

  private def twoStepFetch(nextToken: Option[Either[String, CoverageEntry]],
                           session: RiakSession): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
    nextLocationChunk(nextToken, session) match {
      case (_, Nil) if coverageEntriesIt.hasNext => nextChunk(None, session)
      case (token, locations) =>
        val chunkedLocationsIt = locations.grouped(session.minConnectionsPerNode)
        val dataBuffer = fetchValues(session, chunkedLocationsIt)
        token match {
          case Some(Left(nxt)) => Some(Left(nxt)) -> dataBuffer
          case _ if coverageEntriesIt.hasNext => Some(Right(coverageEntriesIt.next())) -> dataBuffer
          case _ => None -> dataBuffer
        }
    }
  }

  override protected def nextLocationChunk(nextToken: Option[_],
                                           session: RiakSession): (Option[Either[String, CoverageEntry]], Iterable[Location]) = {
    assert(index.isDefined)
    query2iKey = nextToken match {
      case Some(_) if query2iKey.isDefined => query2iKey
      case None => Some(new Query2iKeySingleOrRange[CoverageEntry](bucket, readConf, index.get, coverageEntry.get))
    }
    query2iKey.get.nextLocationChunk(nextToken, session)
  }
}