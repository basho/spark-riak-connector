package com.basho.spark.connector.query

import com.basho.riak.client.api.RiakClient
import com.basho.riak.client.core.query.Location
import com.basho.spark.connector.rdd.{ReadConf, BucketDef}

/**
 * Generic Riak Query
 */
trait Query[TokenT] extends Serializable {
  def bucket: BucketDef
  def readConf: ReadConf

  def nextLocationBulk(nextToken: Option[_], session: RiakClient): (Option[TokenT], Iterable[Location])
}
