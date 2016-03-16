package com.basho.riak.stub

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels._

import com.basho.riak.client.core.RiakMessage
import com.basho.riak.client.core.util.HostAndPort
import shaded.com.basho.riak.protobuf.RiakKvPB
import shaded.com.basho.riak.protobuf.RiakMessageCodes._
import shaded.com.google.protobuf.ByteString

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Forwards all Riak messages to real Riak node
  *
  * @param hostAndPort instance of com.basho.riak.client.core.util.HostAndPort which points to real Riak node
  */
class ProxyMessageHandler(hostAndPort: HostAndPort) extends RiakMessageHandler {

  private final val riakAddress = new InetSocketAddress(hostAndPort.getHost, hostAndPort.getPort)

  override def handle(context: ClientHandler.Context, input: RiakMessage): Iterable[RiakMessage] = input.getCode match {
    // coverage plan received from real Riak node must be modified to replace real node's host and port with proxy
    case MSG_CoverageReq => forwardAndTransform(context, input) { output =>
      val resp = RiakKvPB.RpbCoverageResp.parseFrom(output.getData)
      val modified = RiakKvPB.RpbCoverageResp.newBuilder(resp)
        .clearEntries()
        .addAllEntries(resp.getEntriesList.map { ce =>
          val ceBuilder = RiakKvPB.RpbCoverageEntry.newBuilder(ce)
          if (ce.getIp.toStringUtf8 == hostAndPort.getHost && ce.getPort == hostAndPort.getPort) {
            val localAddress = context.channel.asInstanceOf[NetworkChannel]
              .getLocalAddress.asInstanceOf[InetSocketAddress]
            ceBuilder.setIp(ByteString.copyFromUtf8(localAddress.getHostString))
            ceBuilder.setPort(localAddress.getPort)
          }
          ceBuilder.build()
        }).build()
      new RiakMessage(output.getCode, modified.toByteArray)
    }
    case _ => forwardMessage(context, input)
  }

  private def forwardMessage(context: ClientHandler.Context, input: RiakMessage): Iterable[RiakMessage] = {
    def readRiakResponse(channel: SocketChannel, out: List[RiakMessage] = Nil): Iterable[RiakMessage] = out match {
      case _ if !isDoneReceived(out, input) => readRiakResponse(channel, out ++ readSocket(channel))
      case _ => out
    }

    val channel = SocketChannel.open(riakAddress)
    try {
      // forward request to real Riak node
      assert(channel.write(RiakMessageEncoder.encode(input)) > 0)

      // read response for forwarded request from real Riak node
      readRiakResponse(channel)
    } finally {
      channel.close()
    }
  }

  private def readSocket(channel: SocketChannel): Iterable[RiakMessage] = {
    var accumulator = ByteBuffer.allocateDirect(0)

    var out = ArrayBuffer[RiakMessage]()
    while (out.isEmpty || accumulator.hasRemaining) {
      // try to parse riak message from bytes in accumulator buffer
      RiakMessageEncoder.decode(accumulator) match {
        case Some(x) =>
          accumulator = accumulator.slice()
          out += x
        case None =>
          // read next chunk of data from channel and add it into accumulator
          val in = ByteBuffer.allocateDirect(1024) // scalastyle:ignore
          channel.read(in)
          accumulator = ByteBuffer
            .allocate(accumulator.rewind().limit() + in.flip().limit())
            .put(accumulator)
            .put(in)
          accumulator.rewind()
          in.clear()
      }
    }
    out
  }

  private def isDoneReceived(out: Iterable[RiakMessage], input: RiakMessage): Boolean = input.getCode match {
    case MSG_IndexReq => out.foldLeft[Boolean](false)((a, m) => a || RiakKvPB.RpbIndexResp.parseFrom(m.getData).getDone)
    case _ => out.nonEmpty
  }

  private def forwardAndTransform(context: ClientHandler.Context, input: RiakMessage
                                 )(transform: RiakMessage => RiakMessage
                                 ): Iterable[RiakMessage] = forwardMessage(context, input).map(transform(_))

  override def onRespond(input: RiakMessage, output: Iterable[RiakMessage]): Unit = {}
}