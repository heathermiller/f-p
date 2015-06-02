package silt
package netty

import scala.spores._

import scala.language.existentials
import scala.language.higherKinds

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.util.ReferenceCountUtil

import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup

import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import java.io.ByteArrayOutputStream
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.CountDownLatch
import java.net.SocketAddress

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}

import Implicits._

import scala.pickling._
import Defaults._
import binary._


// For each channel a separate `ClientHandler` is instantiated
abstract class ClientHandler extends ChannelInboundHandlerAdapter with SendUtils {
  var chunkStatus: Option[ReadStatus] = None

  def unpickleAndHandle(arr: Array[Byte]): Unit = {
    // PICKLING
    val pickle = BinaryPickle(arr)
    val command = pickle.unpickle[Any]
    println(s"CLIENT: received $command")
    command match {
      case theMsg @ OKCreated(name) =>
        // response to request, so look up promise
        val p = systemImpl.promiseOf(theMsg.id)
        p.success(theMsg)
      case theMsg @ ForceResponse(value) =>
        // response to request, so look up promise
        val p = systemImpl.promiseOf(theMsg.id)
        p.success(theMsg)        
      case _ =>
        /* do nothing */
    }
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    println(s"CLIENT: enter channelRead: ${ctx.channel()}")

    val in: ByteBuf = msg.asInstanceOf[ByteBuf]
    val bos = new ByteArrayOutputStream
    try {
      while (in.isReadable()) {
        bos.write(in.readByte().asInstanceOf[Int])
      }
    } finally {
      ReferenceCountUtil.release(msg)
    }

    var chunk = bos.toByteArray()

    chunkStatus match {
      case Some(PartialStatus(prevArr)) =>
        chunk = prevArr ++ chunk
        chunkStatus = None
      case _ =>
        // do nothing (handle later)
    }

    var finished = false
    while (!finished) {
      chunkStatus match {
        case None if chunk.length < 4 =>
          finished = true
          chunkStatus = Some(PartialStatus(chunk))

        case None => // have received first chunk
          // read length (first 4 bytes)
          var maxSize: Int = 0
          maxSize |= (chunk(0) << 24)
          maxSize |= (chunk(1) << 16) & 0xFF0000
          maxSize |= (chunk(2) << 8 ) & 0xFF00
          maxSize |= (chunk(3)      ) & 0xFF
          // chunk complete?
          if (chunk.length == maxSize + 4) {
            // println(s"CHUNK: read chunk has EXACT size [$maxSize bytes]")
            finished = true
            unpickleAndHandle(chunk.drop(4))
          } else if (chunk.length > maxSize + 4) {
            val regularChunk = chunk.slice(4, maxSize + 4) //TODO: PERF
            unpickleAndHandle(regularChunk)
            // new chunk to process next in the loop
            chunk = chunk.drop(maxSize + 4)
          } else {
            // println(s"CHUNK: read chunk has SMALLER size [read: ${chunk.length - 4} bytes, max: $maxSize bytes]")
            finished = true
            val done = Promise[Array[Byte]]()
            done.future.foreach(unpickleAndHandle)
            chunkStatus = Some(ChunkStatus(maxSize, chunk.length - 4, ArrayBuffer(chunk.drop(4)), done))
          }

        case Some(status @ ChunkStatus(maxSize, size, chunks, done)) =>
          val newSize = size + chunk.length
          if (newSize < maxSize) {
            chunks += chunk
            chunkStatus = Some(status.copy(currentSize = newSize))
            finished = true
          } else if (newSize == maxSize) {
            chunks += chunk
            chunkStatus = None
            val arr = chunks.flatten.toArray
            done.success(arr)
            finished = true
          } else if (newSize > maxSize) {
            // chunk.length is too much
            // use only maxSize - size elements from the current chunk
            val useOnly = maxSize - size
            val regularChunk = chunk.take(useOnly) //TODO: PERF
            // new chunk to process next in the loop
            chunk = chunk.drop(useOnly)
            chunks += regularChunk
            chunkStatus = None
            val arr = chunks.flatten.toArray
            done.success(arr)
          }
      }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}


trait HasNames {
  var name: String = _
  var newName: String = _
}

// this is what we ultimately want to use
case class TransformerMessage[A, B, C[_] <: Traversable[_]](combinator: Transformer[A, B, C], spore: A => B)
  extends AbstractMessage[C[A], A, B, C[B]]

case class CombinerMessage[A, B, C[_] <: Traversable[_]](combinator: Combiner[A, B, C], spore: A => B)
  // extends AbstractMessage[C[A], A, B, B]

case class AccessorMessage[A, B, C[_] <: Traversable[_]](combinator: Accessor[A, B, C], spore: A => B)
  extends AbstractMessage[C[A], A, B, B]

/*
  type Transformer[A, B, C[_] <: Traversable[_]] = (C[A], A => B) => C[B] // stateless

  type Combiner[A, B, C[_] <: Traversable[_]] = (C[A], B, (B, A) => B) => B // stateless

  type Accessor[A, B, C[_] <: Traversable[_]] = (C[A], A => B) => B // stateless
*/

// show as second step: generalizing `Message`
// in the most general case: no relationship between types `A` and `B`
abstract class AbstractMessage[A, B, C, D] extends HasNames {

  // data of type `A`
  // user spore of type `Spore[B, C]`
  def combinator: (A, B => C) => D

  def spore: B => C

}
