package com.basho.riak.spark.rdd.partitioner

object PartitioningUtils {
  def splitListEvenly[A](list: Seq[A], splitCount: Int) = {
    val (base, rem) = divide(list.size, splitCount)
    val (smaller, bigger) = list.splitAt(list.size - rem * (base + 1))
    smaller.grouped(base) ++ bigger.grouped(base + 1)
  }

  // e.g. split 64 coverage entries into 10 partitions: (6,6,6,6,6,6,7,7,7,7) coverage entries in partitions respectively
  def distributeEvenly(size: Int, splitCount: Int): Seq[Int] = {
    val (base, rem) = divide(size, splitCount)
    for (i <- 0 until splitCount)
      yield if (i < rem) base + 1 else base
  }

  def divide(size: Int, splitCount: Int) = {
    (size / splitCount, size % splitCount)
  }
}