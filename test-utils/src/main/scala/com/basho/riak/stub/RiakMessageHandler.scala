package com.basho.riak.stub

import com.basho.riak.client.core.RiakMessage

/**
  * A tagging interface that all Riak message handlers must extend. The class that is interested in processing an
  * Riak message implements this interface. When the Riak message receives, that object's <code>handle</code> method is
  * invoked.
  */
trait RiakMessageHandler {

  /**
    * Invoked when an Riak message receives.
    */
  def handle(context: ClientHandler.Context, input: RiakMessage): Iterable[RiakMessage]

  /**
    * Invoked when response was sent to client. According to business logic, multiple messages could be sent to client
    * as a response for particular input.
    *
    * @param input  request message
    * @param output response messages
    */
  def onRespond(input: RiakMessage, output: Iterable[RiakMessage]): Unit
}
