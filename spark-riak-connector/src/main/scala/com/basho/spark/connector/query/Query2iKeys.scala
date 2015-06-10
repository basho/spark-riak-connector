package com.basho.spark.connector.query

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.Location
import com.basho.spark.connector.rdd.{BucketDef, ReadConf}
import org.apache.spark.Logging

import scala.collection.mutable.ArrayBuffer

private case class Query2iKeys[K](bucket: BucketDef, readConf:ReadConf, index: String, keys: Iterable[K]) extends QuerySubsetOfKeys[K] with Logging{
  private var query2iKey: Option[Query2iKeySingleOrRange[K]] = None
  private var tokenNext: Option[String] = None

  // By default there should be an empty Serializable Iterator
  private var _iterator: Iterator[Location] = ArrayBuffer.empty[Location].iterator

  private def bulkIsCollected(bulk: Iterable[Location]) = bulk.size >= readConf.fetchSize

  override def locationsByKeys(keys: Iterator[K], session: RiakClient): Iterable[Location] = {
    val dataBuffer = new ArrayBuffer[Location](readConf.fetchSize)

    while ((keys.hasNext || _iterator.hasNext) && !bulkIsCollected(dataBuffer)){
      // Previously gathered results should be returned at first, if any
      _iterator exists ( location => {
        dataBuffer += location
        bulkIsCollected(dataBuffer)
      })

      if(!bulkIsCollected(dataBuffer)) tokenNext match {
        case Some(next) =>
          // Fetch the next results page from the previously executed 2i query, if any
          assert(query2iKey.isDefined)

          val r = query2iKey.get.nextLocationBulk( tokenNext, session)
          tokenNext = r._1
          _iterator = r._2.iterator

        case None if keys.hasNext =>
          // query data for the first/next key
          assert(_iterator.isEmpty && tokenNext.isEmpty)

          val key = keys.next()
          query2iKey = Some(new Query2iKeySingleOrRange[K](bucket, readConf, index, key))

          val r = query2iKey.get.nextLocationBulk(tokenNext, session)
          tokenNext = r._1
          _iterator = r._2.iterator

        case _ => // There is nothing to do
      }
    }
    dataBuffer
  }
}
