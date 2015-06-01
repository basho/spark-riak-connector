package com.basho.spark.connector.query

import com.basho.riak.client.api.cap.Quorum

import scala.collection.JavaConversions._

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.api.commands.kv.{FetchValue, MultiFetch}
import com.basho.riak.client.core.query.{RiakObject, Location}
import com.basho.spark.connector.util.Logging

import scala.collection.mutable.ArrayBuffer

class DataQueryingIterator(query: Query[_], riakSession: RiakClient)
  extends Iterator[(Location, RiakObject)] with Logging {

  type ResultT = (Location, RiakObject)

  // The following 2 variables are useful only for the debugging purposes
  private val dataBuffer: ArrayBuffer[ResultT] = new ArrayBuffer[ResultT](query.readConf.fetchSize)
  private var bufferIndex = 0

  private var isThereANextValue: Any = None
  private var nextToken: Option[_] = None
  private var _iterator: Iterator[ResultT] = null

  private[this] def prefetchIfNeeded(): Boolean = {
    if (this._iterator != null) {
      if(this._iterator.hasNext){
        // Nothing to do, we still have at least one result
        logTrace("prefetch is not required, at least one prefetched value is in the buffer")
        return false
      }else if(nextToken.isEmpty){
        // Nothing to do, we already got all the data
        logDebug("prefetch is not required, all data was processed")
        return false
      }
    }
    logTrace(s"Performing 2i query(token=$nextToken)")

    val r = query.nextLocationBulk(nextToken, riakSession )
    logDebug(s"2i query(token=${nextToken}) returns:\n  token: ${r._1}\n  locations: ${r._2}")
    nextToken = r._1

    dataBuffer.clear()
    bufferIndex = 0

    if(r._2.isEmpty){
      /**
       * It is Absolutely possible situation, for instance:
       *     in case when the last data page will be returned as a result of  2i continuation query and
       *     this page will be fully filled with data then the valid continuation token wile be also returned (it will be not null),
       *     therefore additional/subsequent data fetch request will be required.
       *     As a result of such call the empty locations list and Null continuation token will be returned
       */
      logDebug("prefetch is not required, all data was processed (location list is empty)")
      _iterator = Iterator.empty
      return true
    }

    // fetching actual objects
    val builder = new MultiFetch.Builder()
      .withOption(FetchValue.Option.R, Quorum.oneQuorum())

    r._2.foreach(builder.addLocation)

    val mfr = riakSession.execute(builder.build())

    for (f <- mfr.getResponses) {
      val r = f.get()
      val location = f.getQueryInfo

      if (r.isNotFound) {
        // TODO: add proper error handling
        logError(s"Nothing was found for location '${f.getQueryInfo.getKeyAsString}'")
      } else if (r.hasValues) {
        if (r.getNumberOfValues > 1) {
          throw new IllegalStateException(s"Fetch for '$location' returns more than one result: ${r.getNumberOfValues} actually")
        }

        val ro = r.getValue(classOf[RiakObject])
        dataBuffer += ((location, ro))
      } else {
        logWarning(s"There is no value for location '$location'")
      }
    }

    logDebug(s"Next data buffer was fetched:\n" +
      s"\tnextToken: $nextToken\n" +
      s"\tbuffer: $dataBuffer")

    _iterator = dataBuffer.iterator
    true
  }

  override def hasNext: Boolean =
    isThereANextValue match {
      case b: Boolean =>
        b
      case _ =>
        prefetchIfNeeded()
        val r= _iterator.hasNext
        isThereANextValue = r
        r
    }

  override def next(): (Location, RiakObject) = {
    if( !hasNext ){
      throw new NoSuchElementException("next on iterator")
    }

    bufferIndex += 1
    isThereANextValue = None
    _iterator.next()
  }
}
