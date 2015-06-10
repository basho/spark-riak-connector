package com.basho.spark.connector.query

case class RiakKeys[T](
  keysOrRange: Either[Seq[T],Seq[(T, Option[T])]],
  // Might be None if represents the bucket keys
  index: Option[String] = None
)

object RiakKeys {
  def create2iKeyRanges[K](index: String, ranges: (K, Option[K])*):RiakKeys[K] ={
    new RiakKeys[K](Right(ranges), Some(index))
  }

  def create2iKeys[K](index: String, keys: K*):RiakKeys[K] ={
    new RiakKeys[K](Left(keys), Some(index))
  }

  def createBucketKeys(keys: String*):RiakKeys[String] ={
    new RiakKeys[String](Left(keys))
  }
}