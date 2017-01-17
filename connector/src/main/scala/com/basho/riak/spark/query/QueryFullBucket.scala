package com.basho.riak.spark.query

import com.basho.riak.client.api.commands.indexes.BinIndexQuery
import com.basho.riak.client.api.commands.kv.CoveragePlan.Builder
import com.basho.riak.client.api.commands.kv.{CoveragePlan, FullBucketRead}
import com.basho.riak.client.core.NoNodesAvailableException
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{Location, Namespace, RiakObject}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.JavaConversions._
import scala.util.control.Exception._

case class QueryFullBucket(bucket: BucketDef,
                           readConf: ReadConf,
                           riakConnector: RiakConnector,
                           coverageEntries: Iterable[CoverageEntry],
                           index: Option[String] = None
                          ) extends DirectLocationQuery[String] {

  override protected val coverageEntriesIt: Option[Iterator[CoverageEntry]] = Some(coverageEntries.iterator)
  val unavailableEntries: List[CoverageEntry] = Nil

  override def nextChunk(nextToken: Option[Either[String, CoverageEntry]]
                        ): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {

    def nextChunk(nextToken: Option[Either[String, CoverageEntry]],
                  unavailableEntries: List[CoverageEntry]
                 ): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {
      allCatch either fetchChunk(nextToken) match {
        case Right(v) => v

        /* If NoNodesAvailableException was caught it's a signal that current Riak node is unavailable and it's
         * necessary to obtain alternative coverage entry for another node which contains data replications
         * and re-query chunk using that alternative coverage entry */
        case Left(e) if ExceptionUtils.getRootCause(e).isInstanceOf[NoNodesAvailableException] =>
          val unavailableList = coverageEntry.get :: unavailableEntries
          logWarning(s"Node '${primaryHost.get}' is not available. Alternative coverage entry must be requested.")
          logDebug(s"Unable to execute query using current coverage entry: ${coverageEntry.get}.")
          val cmd = new Builder(bucket.asNamespace())
            .withMinPartitions(readConf.splitCount)
            .withReplaceCoverageEntry(coverageEntry.get)
            .withUnavailableCoverageEntries(unavailableList)
            .build()
          coverageEntry = riakConnector.withSessionDo(s => s.execute(cmd)) match {
            case response: CoveragePlan.Response if response.iterator.hasNext =>
              val alternative = response.iterator().next()
              logDebug(s"Alternative coverage entry ($alternative) instead of (${coverageEntry.get}) is received.")
              Some(alternative)
            case _ => throw new IllegalStateException("Unable to get alternative coverage plan")
          }
          nextChunk(nextToken, unavailableList)
        case Left(e) => throw e
      }
    }

    coverageEntry = nextCoverageEntry(nextToken)
    nextChunk(nextToken, List())
  }

  private def fetchChunk(nextToken: Option[Either[String, CoverageEntry]]) = {
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
        token match {
          case Some(Left(_)) => token -> values
          case _ if coverageEntriesIt.get.hasNext => Some(Right(coverageEntriesIt.get.next)) -> values
          case _ => None -> values
        }
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
        val data = entries.map(e => e.getFetchedValue.getLocation -> e.getFetchedValue.getValue(classOf[RiakObject]))
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