package com.basho.riak.spark.query

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.FullBucketRead
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.query.{RiakObject, Location}
import com.basho.riak.client.core.util.BinaryValue
import com.basho.riak.spark.rdd.{BucketDef, ReadConf}

import scala.collection.JavaConversions._

case class QueryFullBucket(bucket: BucketDef, readConf: ReadConf, coverageEntries: Iterable[CoverageEntry]) extends Query[String] {
  override def nextChunk(nextToken: Option[_], session: RiakClient): (Option[String], Iterable[(Location, RiakObject)]) = {
    require(coverageEntries.size == 1, "Multiple coverage entries hasn't been tested yet")

    val builder = new FullBucketRead.Builder(bucket.asNamespace(), coverageEntries.head.getCoverageContext)
      .withMaxResults(readConf.fetchSize)
      .withPaginationSort(true)
      .withReturnBody(true)

    nextToken match {
      case None =>
      /* It is a first request */

      case Some(continuation: String) =>
        /* Subsequent request */
        builder.withContinuation(BinaryValue.create(continuation))

      case _ =>
        throw new IllegalArgumentException("Wrong nextToken")
    }

    val r: FullBucketRead.Response = session.execute(builder.build())
    val data = for {
      e <- r.getEntries
      n = (e.getLocation, e.getFetchedValue.getValue(classOf[RiakObject]))
    } yield n

    if(!r.hasContinuation){
      None -> data
    }else{
      Some(r.getContinuation.toStringUtf8) -> data
    }
  }
}
