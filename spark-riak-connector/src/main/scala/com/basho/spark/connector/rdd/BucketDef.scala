package com.basho.spark.connector.rdd

import com.basho.riak.client.core.query.Namespace

/**
 * Will be later extended with the partition information
 */
case class BucketDef(bucketType: String,
                    bucketName: String){

  def asNamespace() = new Namespace(bucketType, bucketName)
}

object BucketDef{
  def apply(bucketName: String) = new BucketDef("default", bucketName)
  def apply(ns: Namespace) = new BucketDef(ns.getBucketTypeAsString, ns.getBucketNameAsString)
}
