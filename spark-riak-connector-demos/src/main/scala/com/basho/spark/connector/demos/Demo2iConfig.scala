package com.basho.spark.connector.demos

import com.basho.riak.client.core.RiakNode
import com.basho.riak.client.core.query.Namespace
import com.basho.spark.connector.rdd.RiakConnectorConf
import org.apache.spark.SparkConf

case class Demo2iConfig(riakConf: RiakConnectorConf, index: String, bucket: String, from: Long, to: Long, name:String){
  val riakNamespace = new Namespace("default", bucket)

  def riakNodeBuilder(minConnections:Int = 2):RiakNode.Builder = {
    val firstTheWinner = riakConf.hosts.iterator.next()

    new RiakNode.Builder()
      .withMinConnections(minConnections)
      .withRemoteAddress(firstTheWinner.getHostText)
      .withRemotePort(firstTheWinner.getPort)
  }
}

object Demo2iConfig{

  val DEFAULT_INDEX_NAME = "creationNo"
  val DEFAULT_BUCKET_NAME = "test-bucket"
  val DEFAULT_FROM = 1
  val DEFAULT_TO = 4

  def apply(sparkConf: SparkConf):Demo2iConfig = {

    Demo2iConfig(
      riakConf = RiakConnectorConf(sparkConf),
      index = sparkConf.get("spark.riak.demo.index", DEFAULT_INDEX_NAME),
      bucket = sparkConf.get("spark.riak.demo.bucket", DEFAULT_BUCKET_NAME),
      from = sparkConf.get("spark.riak.demo.from", DEFAULT_FROM.toString).toLong,
      to = sparkConf.get("spark.riak.demo.to", DEFAULT_TO.toString).toLong,
      name = sparkConf.get("spark.app.name", "")
    )
  }
}
