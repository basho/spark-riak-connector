package com.basho.riak.spark.rdd.partitioner

import java.util

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark.rdd.{ReadConf, RiakKVTests, RiakRDD}
import org.junit.Assert._
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakKVTests]))
class FinePartitioningTest(splitSize: Int) extends AbstractCoveragePlanBasedPartitionerTest {

  final val COVERAGE_ENTRIES_COUNT = 64

  @Test
  def checkFinePartitioning(): Unit = {
    val fakeCoverageData = for {
      i <- 0 until splitSize
      j <- 0 until COVERAGE_ENTRIES_COUNT
      val e = s"h$i" -> s"$j"
    } yield e

    mockKVCoveragePlan(fakeCoverageData: _*)

    val rdd = new RiakRDD(sc, rc, "default", "test", None, ReadConf(_splitCount=Some(splitSize))).queryAll()
    val partitions = rdd.partitions

    // verify partitions number
    assertEquals(splitSize, partitions.length)

    // verify that each coverage entity contains in corresponding partition
    partitions.foreach {
      case p: RiakLocalCoveragePartition[_] => p.queryData.coverageEntries match {
        case Some(es) => es.foreach(e => HostAndPort.fromParts(e.getHost, e.getPort) == p.primaryHost)
        case None => fail("Coverage entries must be present")
      }
      case _ => fail(s"Returned partitions must be instances of ${classOf[RiakLocalCoveragePartition[_]].getName}")
    }
  }
}

object FinePartitioningTest {

  @Parameters(name = "Split To {0} partitions")
  def parameters: util.Collection[Array[Integer]] = {
    new util.ArrayList[Array[Integer]]() {
      {
        add(Array(12))
        add(Array(9))
        add(Array(6))
        add(Array(5))
        add(Array(3))
      }
    }
  }
}
