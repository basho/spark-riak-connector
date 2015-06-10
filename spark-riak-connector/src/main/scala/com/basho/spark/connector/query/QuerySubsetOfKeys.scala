package com.basho.spark.connector.query

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.Location
import org.apache.spark.Logging

private trait QuerySubsetOfKeys[K] extends Query[Int] with Logging{
  def keys: Iterable[K]
  private var _iterator: Option[Iterator[K]] = None
  private var _nextPos: Int = -1

  def locationsByKeys(keys: Iterator[K], session: RiakClient): (Iterable[Location])

  // scalastyle:off cyclomatic.complexity
  final override def nextLocationBulk(nextToken: Option[_], session: RiakClient): (Option[Int], Iterable[Location]) = {
    nextToken match {
      case None | Some(0) =>
        // it is either the first call or a kind of "random" read request of reading the first bulk
        _iterator = Some(keys.iterator) // grouped readConf.fetchSize
        _nextPos = 0

      case Some(requested: Int) if requested == _nextPos =>
      // subsequent read request, there is nothing to do

      case Some(requested: Int) if requested != 0 && requested != _nextPos =>
        // random read request, _iterator should be adjusted
        logWarning(s"nextLocationBulk: random read was requested, it may cause performance issue:\n" +
          s"\texpected position: ${_nextPos}, while the requested read position is $requested")
        _nextPos = requested -1
        _iterator = Some(keys.iterator.drop(_nextPos)) // grouped(readConf.fetchSize)

      case _ =>
        throw new IllegalArgumentException("Wrong nextToken")
    }

    assert(_iterator.isDefined)

    if(!_iterator.get.hasNext){
      // TODO: Add proper error handling
      throw new IllegalStateException()
    }

    val locations = locationsByKeys(_iterator.get, session)
    _nextPos += locations.size

    ( _iterator.get.hasNext match {
      case true => Some(_nextPos)
      case _ => None
    }, locations)
  }
  // scalastyle:off cyclomatic.complexity
}
