package com.basho.riak.spark.query

import com.basho.riak.client.api.commands.indexes.BinIndexQuery
import com.basho.riak.client.api.commands.kv.FullBucketRead
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{Location, Namespace, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}

import scala.collection.JavaConversions._

case class QueryFullBucket(bucket: BucketDef,
                           readConf: ReadConf,
                           riakConnector: RiakConnector,
                           coverageEntries: Iterable[CoverageEntry],
                           index: Option[String] = None
                          ) extends DirectLocationQuery[String] {

  override protected val coverageEntriesIt: Option[Iterator[CoverageEntry]] = Some(coverageEntries.iterator)

  override def nextChunk(nextToken: Option[Either[String, CoverageEntry]]
                        ): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
    coverageEntry = nextCoverageEntry(nextToken)

    if (readConf.useStreamingValuesForFBRead) {
      // Direct fetch is using for BDP version where FullBucketRead query is supported
      oneStepFetch(nextToken)
    } else {
      /* Indirect fetch is using for TS/KV Riak version. Data will be received using sequence of queries:
         Query2iKeySingleOrRange - for fetching locations and MultiFetch for receiving values by known locations */
      twoStepFetch(nextToken)
    }
  }

  private def twoStepFetch(nextToken: Option[Either[String, CoverageEntry]]
                          ): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
    nextLocationChunk(nextToken) match {
      case (_, Nil) if coverageEntriesIt.get.hasNext => nextChunk(None)
      case (token, locations) =>
        val values = fetchValues(locations)
        (token match {
          case Some(Left(_)) => token
          case _ if coverageEntriesIt.get.hasNext => Some(Right(coverageEntriesIt.get.next))
          case _ => None
        }) -> values
    }
  }

  private def oneStepFetch(nextToken: Option[Either[String, CoverageEntry]]
                          ): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
    val builder = new FullBucketRead.Builder(bucket.asNamespace(), coverageEntry.get.getCoverageContext)
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)
      .withReturnBody(true)

    nextToken.foreach {
      case Left(continuation: String) => builder.withContinuation(BinaryValue.create(continuation))
      case _ =>
    }

    val response: FullBucketRead.Response = riakConnector
      .withSessionDo(primaryHost.map(Seq(_)))(session => session.execute(builder.build()))
    response.getEntries.toList match {
      case Nil if coverageEntriesIt.get.hasNext => nextChunk(None)
      case entries: List[FullBucketRead.Response.Entry] =>
        val data = entries.map(e => e.getLocation -> e.getFetchedValue.getValue(classOf[RiakObject]))
        response.hasContinuation match {
          case true => Some(Left(response.getContinuation.toStringUtf8)) -> data
          case _ if coverageEntriesIt.get.hasNext => Some(Right(coverageEntriesIt.get.next)) -> data
          case _ => None -> data
        }
    }
  }

  override protected def nextLocationChunk(nextToken: Option[Either[String, CoverageEntry]]
                                          ): (Option[Either[String, CoverageEntry]], Iterable[Location]) = {
    assert(index.isDefined)
    val ns = new Namespace(bucket.bucketType, bucket.bucketName)
    val builder = new BinIndexQuery.Builder(ns, index.get, coverageEntry.get.getCoverageContext)
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)

    nextToken.foreach {
      case Left(continuation: String) => builder.withContinuation(BinaryValue.create(continuation))
      case _ =>
    }

    val response = riakConnector.withSessionDo(primaryHost.map(Seq(_))) { s => s.execute(builder.build()) }
    val locations = response.getEntries.map(_.getRiakObjectLocation)
    response.hasContinuation match {
      case true => Some(Left(response.getContinuation.toStringUtf8)) -> locations
      case _ => None -> locations
    }
  }
}