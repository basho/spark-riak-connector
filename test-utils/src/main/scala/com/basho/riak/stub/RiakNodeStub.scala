package com.basho.riak.stub

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousCloseException, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}

import com.basho.riak.client.core.util.HostAndPort
import com.basho.riak.stub.RiakNodeStub._
import org.slf4j.LoggerFactory

class RiakNodeStub(val host: String, val port: Int, messageHandler: RiakMessageHandler) {

  private final val localAddress = new InetSocketAddress(host, port)
  private final val clientHandler = new ClientHandler(messageHandler)

  private var serverChannel: AsynchronousServerSocketChannel = _
  private var clients: List[AsynchronousSocketChannel] = Nil

  def start(): HostAndPort = {
    serverChannel = AsynchronousServerSocketChannel.open()
    require(serverChannel.isOpen)

    serverChannel.bind(localAddress)
    serverChannel.accept(serverChannel, new CompletionHandler[AsynchronousSocketChannel, AsynchronousServerSocketChannel]() {
      override def completed(client: AsynchronousSocketChannel, server: AsynchronousServerSocketChannel): Unit = {
        logger.info(s"Incoming connection: ${SocketUtils.serverConnectionAsStr(client)}")
        this.synchronized {
          clients = client :: clients
        }

        val buffer = ByteBuffer.allocateDirect(1024) // scalastyle:ignore
        client.read(buffer, (client, buffer), clientHandler)

        server.accept(server, this)
      }

      override def failed(exc: Throwable, serverChannel: AsynchronousServerSocketChannel): Unit = exc match {
        case _: AsynchronousCloseException =>
        case _ => logger.error(s"Something went wrong:  ${serverChannel.toString}", exc);
      }
    })

    HostAndPort.fromParts(
      serverChannel.getLocalAddress.asInstanceOf[InetSocketAddress].getHostString,
      serverChannel.getLocalAddress.asInstanceOf[InetSocketAddress].getPort)
  }

  def stop(): Unit = this.synchronized {
    Option(serverChannel).foreach(_.close)
    clients.foreach(clientHandler.disconnectClient)
  }
}

object RiakNodeStub {
  val logger = LoggerFactory.getLogger(classOf[RiakNodeStub])
  final val DEFAULT_HOST = "localhost"

  def apply(host: String, port: Int, messageHandler: RiakMessageHandler): RiakNodeStub = new RiakNodeStub(host, port, messageHandler)

  def apply(port: Int, messageHandler: RiakMessageHandler): RiakNodeStub = RiakNodeStub(DEFAULT_HOST, port, messageHandler)

  def apply(messageHandler: RiakMessageHandler): RiakNodeStub = RiakNodeStub(DEFAULT_HOST, 0, messageHandler)
}


