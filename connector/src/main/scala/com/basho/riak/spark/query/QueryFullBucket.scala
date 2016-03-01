package com.basho.riak.spark.query

import com.basho.riak.client.api.commands.kv.FullBucketRead
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{ RiakObject, Location }
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.connector.RiakSession
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}

import scala.collection.JavaConversions._
import com.basho.riak.client.api.commands.kv.FullBucketRead.Response.Entry

case class QueryFullBucket(bucket: BucketDef, readConf: ReadConf, coverageEntries: Iterable[CoverageEntry]) extends Query[Either[String, CoverageEntry]] {

  private val iter = coverageEntries.iterator
  private var ce: Option[CoverageEntry] = None

  override def nextChunk(nextToken: Option[_], session: RiakSession): (Option[Either[String, CoverageEntry]], Iterable[(Location, RiakObject)]) = {

    ce = nextToken match {
      case None                               => Some(iter.next)

      case Some(Left(_: String))                    => ce

      case Some(Right(coverageEntry: CoverageEntry)) => Some(coverageEntry)

      case _ =>
        throw new IllegalArgumentException("Invalid nextToken")
    }
    val builder = new FullBucketRead.Builder(bucket.asNamespace(), ce.get.getCoverageContext)
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)
      .withReturnBody(true)

    nextToken match {
      case Some(Left(continuation: String)) => builder.withContinuation(BinaryValue.create(continuation))
      case _                          =>
    }

    val r: FullBucketRead.Response = session.execute(builder.build())

    val resposeEntries: Seq[Entry] = r.getEntries
    resposeEntries match {
      case Nil if iter.hasNext => nextChunk(None, session)
      case _ =>
        val data = for {
          e <- r.getEntries
          n = (e.getLocation, e.getFetchedValue.getValue(classOf[RiakObject]))
        } yield n

        if (!r.hasContinuation) {
          if (iter.hasNext) {
            Some(Right(iter.next)) -> data
          } else {
            None -> data
          }
        } else {
          Some(Left(r.getContinuation.toStringUtf8)) -> data
        }
    }
  }
}
