package com.basho.riak.stub

import java.nio.channels.AsynchronousSocketChannel

object SocketUtils {

  def clientConnectionAsStr(sock: AsynchronousSocketChannel): String = connectionAsStr(sock, isClient = true)

  def serverConnectionAsStr(sock: AsynchronousSocketChannel): String = connectionAsStr(sock, isClient = false)

  private def connectionAsStr(sock: AsynchronousSocketChannel, isClient: Boolean): String =
    s"${getAddrSafe(sock, isClient)} => ${getAddrSafe(sock, !isClient)}"

  private def getAddrSafe(sock: AsynchronousSocketChannel, localRequired: Boolean): String =
    (if (localRequired) sock.getLocalAddress else sock.getRemoteAddress).toString
}
