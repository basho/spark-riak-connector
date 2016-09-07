package com.basho.riak.stub

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

import com.basho.riak.client.core.netty.RiakMessageCodec
import com.basho.riak.stub.ClientHandler._
import org.slf4j.LoggerFactory

class ClientHandler(val messageHandler: RiakMessageHandler) extends RiakMessageCodec
  with CompletionHandler[Integer, (AsynchronousSocketChannel, ByteBuffer)] {

  override def completed(result: Integer, attachment: (AsynchronousSocketChannel, ByteBuffer)): Unit = attachment match {
    case (channel, buffer) if result > 0 =>
      logger.info(s"Message received ${SocketUtils.serverConnectionAsStr(channel)} ($result bytes).")
      RiakMessageEncoder.decode(buffer.rewind().asInstanceOf[ByteBuffer]) match {
        case Some(m) if channel.isOpen =>
          val msgs = messageHandler.handle(new Context(channel), m)
          val encoded = RiakMessageEncoder.encode(msgs.toSeq: _*)
          val bytes = channel.write(encoded).get
          assert(bytes == encoded.position())
          logger.info(s"Response sent ${SocketUtils.clientConnectionAsStr(channel)} ($bytes bytes).")
          messageHandler.onRespond(m, msgs)
        case Some(m) if !channel.isOpen =>
          logger.warn("Impossible to write message to channel: channel has been already closed")
        case None => // TODO: handle case with no message
      }
      buffer.clear()
      channel.read(buffer, (channel, buffer), this)
    case _ =>
  }

  override def failed(exc: Throwable, attachment: (AsynchronousSocketChannel, ByteBuffer)): Unit = attachment match {
    case (channel, _) if channel.isOpen =>
      logger.error(s"Something went wrong with client ${SocketUtils.serverConnectionAsStr(channel)}", exc)
      disconnectClient(channel)
    case _ => // channel is already closed - do nothing
  }

  def disconnectClient(client: AsynchronousSocketChannel): Unit = this.synchronized {
    client.isOpen match {
      case true =>
        val connectionString = SocketUtils.serverConnectionAsStr(client)
        client.shutdownInput()
        client.shutdownOutput()
        client.close()
        logger.info(s"Client $connectionString was gracefully disconnected")
      case false => // client is already closed - do nothing
    }
  }
}

object ClientHandler {
  val logger = LoggerFactory.getLogger(classOf[ClientHandler])

  /**
    * Conteiner for any additional information which belongs to client handler
    * and could be helpful for handling requests
    */
  class Context(val channel: Channel)

}