package com.basho.riak.spark.rdd

import java.util

import com.basho.riak.client.api.commands.kv.CoveragePlan
import com.basho.riak.client.core.operations.CoveragePlanOperation.Response.CoverageEntry
import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.connector.RiakSession
import com.basho.riak.spark.rdd.partitioner.RiakLocalCoveragePartition
import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.spark.SparkConf
import org.junit.Assert._
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(value = classOf[Parameterized])
@Category(Array(classOf[RiakKVTests]))
class FinePartitioningTest(splitSize: Int) extends AbstractRDDTest {

  final val COVERAGE_ENTRIES_COUNT = 64

  override protected def initSparkConf(): SparkConf = {
    val conf = super.initSparkConf()
    conf.set("spark.riak.input.split.count", splitSize.toString)
  }

  @Test
  def checkFinePartitioning(): Unit = {
    // init hosts map [HostPort -> Seq[CoverageEntry]]
    val hosts = (0 until splitSize)
      .map(i => HostAndPort.fromParts(s"host-$i", 0))
      .map(hp => hp -> (0 until COVERAGE_ENTRIES_COUNT).map { i =>
        val entry = new CoverageEntry
        FieldUtils.writeField(entry, "host", hp.getHost, true)
        FieldUtils.writeField(entry, "port", hp.getPort, true)
        FieldUtils.writeField(entry, "description", s"StubCoverageEntry-$i", true)
        FieldUtils.writeField(entry, "coverageContext", FieldUtils.readField(entry, "description", true).toString.getBytes, true)
        entry
      }).toMap

    val data = sc.riakBucket[String](DEFAULT_NAMESPACE).queryAll()

    val connector = spy(data.connector)
    val session = mock(classOf[RiakSession])
    val coveragePlan = mock(classOf[CoveragePlan.Response])

    doReturn(session).when(connector).openSession(Some(Seq(any(classOf[HostAndPort]))))
    when(session.execute(any(classOf[CoveragePlan]))).thenReturn(coveragePlan)
    when(coveragePlan.hosts()).thenReturn(hosts.keySet)
    when(coveragePlan.iterator()).thenAnswer(new Answer[util.Iterator[_]] {
      override def answer(invocation: InvocationOnMock): util.Iterator[_] = hosts.valuesIterator.asJava
    })
    when(coveragePlan.hostEntries(any(classOf[HostAndPort]))).thenAnswer(new Answer[util.List[CoverageEntry]] {
      override def answer(invocation: InvocationOnMock): util.List[CoverageEntry] =
        hosts(invocation.getArgumentAt(0, classOf[HostAndPort])).asJava
    })

    // copy result RDD and substitute connector mock
    val wrapper = new RiakRDD[String](sc, connector, data.bucketType, data.bucketName, data.queryData, data.readConf)
    val partitions = wrapper.partitions

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
