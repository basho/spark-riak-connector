package com.basho.riak.stub

import com.basho.riak.client.core.RiakMessage
import shaded.com.basho.riak.protobuf.RiakKvPB
import shaded.com.basho.riak.protobuf.RiakKvPB.{RpbIndexReq, _}
import shaded.com.basho.riak.protobuf.RiakMessageCodes._

/**
  * The interface for handling Riak PB requests wrapped in Riak messages.
  * The class that is interested in processing PB request either implements this interface (and all the methods it
  * contains) or extends the abstract <code>RequestBasedMessageAdapter</code> class
  * (overriding only the methods of interest).
  */
trait RequestBasedMessageHandler extends RiakMessageHandler {

  def handle(context: ClientHandler.Context, input: RiakMessage): Iterable[RiakMessage] = input.getCode match {
    case MSG_CoverageReq =>
      val response = handleCoverageRequest(RiakKvPB.RpbCoverageReq.parseFrom(input.getData))
      Seq(new RiakMessage(MSG_CoverageResp, response.toByteArray))

    case MSG_IndexReq =>
      val response = handleIndexRequest(RiakKvPB.RpbIndexReq.parseFrom(input.getData))
      Seq(new RiakMessage(MSG_IndexResp, response.toByteArray))

    case MSG_GetReq =>
      val response = handleGetRequest(RiakKvPB.RpbGetReq.parseFrom(input.getData))
      Seq(new RiakMessage(MSG_GetResp, response.toByteArray))

    case _ => throw new UnsupportedOperationException(s"Message code '${input.getCode}' is not supported.")
  }

  /**
    * Invoked when a Coverage request received.
    */
  def handleCoverageRequest(req: RiakKvPB.RpbCoverageReq): RiakKvPB.RpbCoverageResp

  /**
    * Invoked when a Index request received.
    */
  def handleIndexRequest(req: RiakKvPB.RpbIndexReq): RiakKvPB.RpbIndexResp

  /**
    * Invoked when a Get request received.
    */
  def handleGetRequest(req: RiakKvPB.RpbGetReq): RiakKvPB.RpbGetResp
}

/**
  * An abstract adapter class for handling Riak messages. The methods in this class are empty.
  * This class exists as convenience for creating listener objects.
  *
  * @see RequestBasedMessageHandler
  */
abstract class RequestBasedMessageAdapter extends RequestBasedMessageHandler {

  /**
    * @inheritdoc
    * @note Don't have implementation and throws <code>NotImplementedError</code> by default.
    */
  override def handleCoverageRequest(req: RpbCoverageReq): RpbCoverageResp = throw new NotImplementedError

  /**
    * @inheritdoc
    * @note Don't have implementation and throws <code>NotImplementedError</code> by default.
    */
  override def handleIndexRequest(req: RpbIndexReq): RpbIndexResp = throw new NotImplementedError

  /**
    * @inheritdoc
    * @note Don't have implementation and throws <code>NotImplementedError</code> by default.
    */
  override def handleGetRequest(req: RpbGetReq): RpbGetResp = throw new NotImplementedError

  override def onRespond(input: RiakMessage, iterable: Iterable[RiakMessage]): Unit = {}
}