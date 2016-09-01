package com.basho.riak.spark.rdd.partitioner

import com.basho.riak.client.api.commands.timeseries.CoveragePlan
import com.basho.riak.client.core.query.timeseries.CoverageEntry
import com.basho.riak.client.core.query.timeseries.CoveragePlanResult
import com.basho.riak.spark.rdd.{ReadConf, RegressionTests}
import com.basho.riak.spark.rdd.connector.{RiakConnector, RiakSession}
import org.apache.spark.sql.sources.Filter
import org.junit.Test
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

@RunWith(classOf[MockitoJUnitRunner])
class RiakTSCoveragePlanBasedPartitionerTest {

  @Mock
  private val rc: RiakConnector = null

  @Mock
  private val rs: RiakSession = null

  // To access protected constructor in CoveragePlanResult
  class SimpleCoveragePlanResult extends CoveragePlanResult {
  }

  @Test
  @Category(Array(classOf[RegressionTests]))
  def check(): Unit = {

    val cp = new SimpleCoveragePlanResult()

    cp.addEntry(createCE("1", 1,2))
    cp.addEntry(createCE("2", 3,4))
    cp.addEntry(createCE("2", 5,6))
    cp.addEntry(createCE("2", 7,8))
    cp.addEntry(createCE("3", 9,10))

    doReturn(cp).when(rs).execute(any[CoveragePlan])

    doAnswer( new Answer[CoveragePlanResult] {
      override def answer(invocation: InvocationOnMock): CoveragePlanResult = {
        val func = invocation.getArguments()(0).asInstanceOf[Function1[RiakSession,CoveragePlanResult]]
        func.apply(rs)
      }
    }).when(rc).withSessionDo(any(classOf[Function1[RiakSession,CoveragePlanResult]]))

    val partitioner = new RiakTSCoveragePlanBasedPartitioner(rc, "test", None, None, new Array[Filter](0), new ReadConf())
    val partitions = partitioner.partitions()
  }

  private def createCE(host: String, from: Int, to: Int): CoverageEntry = {
    val ce = new CoverageEntry()
    ce.setFieldName("time")
    ce.setHost(host)
    ce.setLowerBoundInclusive(true)
    ce.setLowerBound(from)
    ce.setUpperBoundInclusive(false)
    ce.setUpperBound(to)

    ce.setDescription(s"table / time >= $from AND time < $to")
    ce
  }
}
