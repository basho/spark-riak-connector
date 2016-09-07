package com.basho.riak.spark.rdd.failover

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.spark._
import com.basho.riak.stub.{RequestBasedMessageAdapter, RiakMessageHandler}
import org.junit.{Ignore, Test}
import org.junit.rules.ExpectedException
import shaded.com.basho.riak.protobuf.RiakKvPB._
import shaded.com.basho.riak.protobuf.RiakPB.RpbPair
import shaded.com.google.protobuf.ByteString

import scala.collection.JavaConverters._

class UnavailableRiakTest extends AbstractFailoverOfflineTest {

  val _expectedException: ExpectedException = ExpectedException.none()

  override val riakHosts: Int = 2

  var stoppedNode: Option[HostAndPort] = None

  val cpResponseIt = new Iterator[RpbCoverageResp] {
    override def hasNext: Boolean = true

    override def next(): RpbCoverageResp = RpbCoverageResp.newBuilder()
      .addAllEntries(riakNodes
        .map { case (hp, _) => hp }
        .filter(!_.equals(stoppedNode))
        .zip(distributeEvenly(COVERAGE_ENTRIES_COUNT, riakHosts))
        .flatMap {
          case (hp, partitionsPerNode) => (0 until partitionsPerNode).map {
            case partitionIndex: Int => RpbCoverageEntry.newBuilder()
              .setIp(ByteString.copyFromUtf8(hp.getHost))
              .setPort(hp.getPort)
              // put host and port into coverage context to identify request's server later
              .setCoverContext(ByteString.copyFromUtf8(hp.toString))
              .setKeyspaceDesc(ByteString.copyFromUtf8(s"StubCoverageEntry-${hp.toString}-$partitionIndex"))
              .build()
          }
        }.asJava)
      .build()
  }

  override val riakMessageHandler: Option[RiakMessageHandler] = Some(new RequestBasedMessageAdapter {
    override def handleCoverageRequest(req: RpbCoverageReq): RpbCoverageResp = cpResponseIt.next()

    override def handleIndexRequest(req: RpbIndexReq): RpbIndexResp = {
      // stop node which is not currently in use
      synchronized {
        riakNodes.find { case (hp, _) => s"${hp.toString}" != req.getCoverContext.toStringUtf8 }
          .foreach {
            case (hp, rs) if stoppedNode.isEmpty =>
              rs.stop()
              logInfo(s"Node '${hp.getHost}:${hp.getPort}' was stopped to simulate node crash")
              stoppedNode = Some(hp)
            case _ =>
          }
      }

      RpbIndexResp.newBuilder()
        .addKeys(ByteString.copyFromUtf8("k0"))
        .setDone(true)
        .build()
    }

    override def handleGetRequest(req: RpbGetReq): RpbGetResp = RpbGetResp.newBuilder()
      .addContent(RpbContent.newBuilder()
        .setValue(ByteString.copyFromUtf8("v0"))
        .setContentType(ByteString.copyFromUtf8("text/plain"))
        .addAllIndexes(List(RpbPair.newBuilder()
          .setKey(ByteString.copyFromUtf8("i0_int"))
          .setValue(ByteString.copyFromUtf8("indexValue"))
          .build()).asJava)
        .build())
      .build()
  })

  // This test could be un-ignored after investigation and correction of health check behavior for failing node
  @Ignore("Ignored due to floating issue connected with infinite amount of health check requests if Riak node is down")
  @Test
  def fullBucketReadWithStoppedNodeShouldPass(): Unit = {
    sc.riakBucket[String](NAMESPACE).queryAll().collect()
  }
}
