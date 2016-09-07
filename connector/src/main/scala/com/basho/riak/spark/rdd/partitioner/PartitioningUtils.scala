package com.basho.riak.spark.rdd.partitioner

object PartitioningUtils {

  val quantaLimit: Int = 4

  def splitListEvenly[A](list: Seq[A], splitCount: Int): Iterator[Seq[A]] = {
    val (base, rem) = divide(list.size, splitCount)
    val (smaller, bigger) = list.splitAt(list.size - rem * (base + 1))

    if (smaller.isEmpty) {
      bigger.grouped(base + 1)
    } else {
      smaller.grouped(base) ++ bigger.grouped(base + 1)
    }
  }

  // e.g. split 64 coverage entries into 10 partitions: (6,6,6,6,6,6,7,7,7,7) coverage entries in partitions respectively
  def distributeEvenly(size: Int, splitCount: Int): Seq[Int] = {
    val (base, rem) = divide(size, splitCount)
    for (i <- 0 until splitCount)
      yield if (i < rem) base + 1 else base
  }
  
  def distributeEvenly(size: Long, splitCount: Int): Seq[Long] = {
    val (base, rem) = divide(size, splitCount)
    for (i <- 0 until splitCount)
      yield if (i < rem) base + 1 else base
  }

  private def divide(size: Long, splitCount: Int): (Long, Long) = {
    (size / splitCount, size % splitCount)
  }
  
  private def divide(size: Int, splitCount: Int): (Int, Int) = {
    (size / splitCount, size % splitCount)
  }

  def splitListIntoGroupes[A](list: Seq[A], distrList: List[Int]): Seq[Seq[A]] = {
    distrList match {
      case Nil => Nil
      case x :: xs => {
        val (group, rest) = list.splitAt(x)
        group +: splitListIntoGroupes(rest, xs)
      }
    }
  }
}