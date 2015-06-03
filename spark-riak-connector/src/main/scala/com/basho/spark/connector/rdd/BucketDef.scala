package com.basho.spark.connector.rdd

import com.basho.riak.client.core.query.Namespace

/**
 * Will be later extended with the partition information
 */
case class BucketDef(bucketType: String,
                    bucketName: String){

  def asNamespace(): Namespace = new Namespace(bucketType, bucketName)
}

object BucketDef{
  def apply(bucketName: String): BucketDef = new BucketDef("default", bucketName)
  def apply(ns: Namespace): BucketDef = new BucketDef(ns.getBucketTypeAsString, ns.getBucketNameAsString)
}
