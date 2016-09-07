package com.basho.riak.spark.streaming

import java.net.InetSocketAddress
import java.nio.channels.{AsynchronousCloseException, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}

import com.basho.riak.stub.SocketUtils
import org.apache.spark.Logging

class SocketStreamingDataSource extends Logging {

  private var serverChannel: AsynchronousServerSocketChannel = _
  private var clientChannel: AsynchronousSocketChannel = _

  def start(writeToSocket: AsynchronousSocketChannel => Unit): Int = {
    serverChannel = AsynchronousServerSocketChannel.open()
    require(serverChannel.isOpen)

    serverChannel.bind(new InetSocketAddress(0))
    serverChannel.accept(serverChannel, new CompletionHandler[AsynchronousSocketChannel, AsynchronousServerSocketChannel]() {
      override def completed(client: AsynchronousSocketChannel, server: AsynchronousServerSocketChannel): Unit = {
        logInfo(s"Incoming connection: ${SocketUtils.serverConnectionAsStr(client)}")
        clientChannel = client

        writeToSocket(client)

        client.isOpen match {
          case true =>
            val connectionString = SocketUtils.serverConnectionAsStr(client)
            client.shutdownInput()
            client.shutdownOutput()
            client.close()
            logInfo(s"Client $connectionString was gracefully disconnected")
          case false => // client is already closed - do nothing
        }
      }

      override def failed(exc: Throwable, serverChannel: AsynchronousServerSocketChannel): Unit = exc match {
        case _: AsynchronousCloseException =>
        case _ => logError(s"Something went wrong:  ${serverChannel.toString}", exc);
      }
    })

    serverChannel.getLocalAddress.asInstanceOf[InetSocketAddress].getPort
  }

  def stop(): Unit = {
    Option(clientChannel).foreach(_.close())
    Option(serverChannel).foreach(_.close())
  }
}
