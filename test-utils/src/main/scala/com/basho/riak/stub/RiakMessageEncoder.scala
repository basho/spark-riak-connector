package com.basho.riak.stub

import java.nio.ByteBuffer
import java.util

import com.basho.riak.client.core.RiakMessage
import com.basho.riak.client.core.netty.RiakMessageCodec
import io.netty.buffer.Unpooled

import scala.collection.JavaConversions._

object RiakMessageEncoder extends RiakMessageCodec {

  def encode(messages: RiakMessage*): ByteBuffer = {
    val buffer = Unpooled.buffer(messages.head.getData.length)
    messages.foreach(encode(null, _, buffer)) // scalastyle:ignore
    buffer.nioBuffer()
  }

  def decode(in: ByteBuffer): Option[RiakMessage] = {
    val out = new util.ArrayList[AnyRef]()
    val buffer = Unpooled.wrappedBuffer(in)
    decode(null, buffer, out) // scalastyle:ignore
    List(out: _*) match {
      case msg :: _ =>
        in.position(buffer.readerIndex())
        Some(msg.asInstanceOf[RiakMessage])
      case Nil => None
    }
  }
}
