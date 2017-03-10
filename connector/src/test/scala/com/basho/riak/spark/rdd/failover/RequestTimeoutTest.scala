package com.basho.riak.spark.rdd.failover

import java.util.concurrent.CountDownLatch

import com.basho.riak.spark._
import com.basho.riak.stub.{RequestBasedMessageAdapter, RiakMessageHandler}
import org.junit.rules.ExpectedException
import org.junit.{After, Rule, Test}
import shaded.com.basho.riak.protobuf.RiakKvPB._
import shaded.com.google.protobuf.ByteString

import scala.collection.JavaConverters._

class RequestTimeoutTest extends AbstractFailoverOfflineTest {

  val _expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def expectedException: ExpectedException = _expectedException

  override val riakMessageHandler: Option[RiakMessageHandler] = Some(new RequestBasedMessageAdapter {
    override def handleCoverageRequest(req: RpbCoverageReq): RpbCoverageResp = RpbCoverageResp.newBuilder()
      .addAllEntries(riakNodes
        .zip(distributeEvenly(COVERAGE_ENTRIES_COUNT, riakHosts))
        .flatMap {
          case ((a, _), partitionsPerNode) => (0 until partitionsPerNode).map {
            case partitionIndex: Int => RpbCoverageEntry.newBuilder()
              .setIp(ByteString.copyFromUtf8(a.getHost))
              .setPort(a.getPort)
              .setCoverContext(ByteString.copyFromUtf8(s"StubCoverageEntry-${a.toString}-$partitionIndex"))
              .setKeyspaceDesc(ByteString.copyFromUtf8(s"StubCoverageEntry-${a.toString}-$partitionIndex"))
              .build()
          }
        }.asJava)
      .build()

    override def handleIndexRequest(req: RpbIndexReq): RpbIndexResp = {
      logInfo("Index Request is going to stuck...")
      latch.await()
      logInfo("Timeout verified. Thread execution continued.")

      RpbIndexResp.newBuilder().build()
    }
  })

  val latch = new CountDownLatch(1)

  @Test(timeout = 5000) // scalastyle:ignore
  def fullBucketReadShouldFailWithTimeout(): Unit = {
    expectedException.expectMessage("test timed out after 5000 milliseconds")
    sc.riakBucket[String](NAMESPACE).queryAll().collect()
  }

  @After
  def after(): Unit = {
    latch.countDown()
  }
}
