package silt
package netty

import scala.pickling._
import Defaults._
import shareNothing._

import _root_.io.netty.channel.Channel
import _root_.io.netty.buffer.ByteBuf

import scala.concurrent.{Future, Promise}
import scala.collection.mutable.ArrayBuffer


trait SendUtils {

  val CHUNK_SIZE = 256

  def systemImpl: SystemImpl

  def pickleWriteFlush[T: Pickler](ch: Channel, msg: T): Unit = {
    val pickler = implicitly[Pickler[T]]
    val tag     = pickler.tag
    println(s"tag: ${tag.key}")

    if (msg.isInstanceOf[graph.Graph] || msg.isInstanceOf[InitSiloFun[_]] || msg.isInstanceOf[InitSiloValue[_]]) {
      import json.pickleFormat
      val builder = pickleFormat.createBuilder()
      builder.hintTag(tag)
      println(s"pickler class: ${pickler.getClass.getName}")
      pickler.pickle(msg, builder)
      val p = builder.result()
      val arr = p.value
      println(s"result: $arr")
      print(s"trying to unpickle... ")
      val up = p.unpickle[Any]
      println(up)
    }

    val builder = binary.pickleFormat.createBuilder()
    builder.hintTag(tag)
    // println(s"pickler class: ${pickler.getClass.getName}")
    try {
      pickler.pickle(msg, builder)
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
    val p = builder.result()
    val arr = p.value

    // chunking

    // allocators "are expected to be thread-safe" (see http://netty.io/4.0/api/io/netty/buffer/ByteBufAllocator.html)
    val allocator = ch.alloc()

    println(s"writing $msg to channel $ch...")
    if (arr.length <= CHUNK_SIZE - 4) { // no chunking
      val buf = allocator.buffer(arr.length + 4)
      buf.writeInt(arr.length)
      buf.writeBytes(arr)
      // TODO: why do we sync() here? isn't this a performance issue?
      ch.writeAndFlush(buf).sync()
    } else {
      val len = arr.length + 4
      val numChunks = len / CHUNK_SIZE
      val rem = len % CHUNK_SIZE

      val allBuffers = ArrayBuffer[ByteBuf]()
      for (_ <- 1 to numChunks) {
        allBuffers += allocator.buffer(CHUNK_SIZE)
      }
      if (rem != 0) allBuffers += allocator.buffer(rem)

      // write array length into first byte buffer
      allBuffers(0).writeInt(arr.length)

      // copy `arr` into byte buffers
      var i = 0
      var currentChunk = 0 // continue writing in first byte buffer
      var currentSize = 4 // have already written array length
      while (i < len - 4) {
        allBuffers(currentChunk).writeByte(arr(i))
        currentSize += 1
        if (currentSize == CHUNK_SIZE) {
          currentChunk += 1
          currentSize = 0
        }
        i += 1
      }

      // write buffers to channel
      for (buf <- allBuffers) {
        ch.write(buf)
      }
      ch.flush()
    }
  }

  def sendToChannel[T: Pickler](ch: Channel, msg: T): Unit = {
    // TODO: what is this lock used for?
    systemImpl.lock.lock()
    pickleWriteFlush(ch, msg)
    systemImpl.lock.unlock()
  }

  def sendWithReply[T <: ReplyMessage : Pickler](ch: Channel, msg: T): Future[Any] = {
    systemImpl.lock.lock()

    val response = Promise[Any]()
    systemImpl.promiseOf += (msg.id -> response)
    pickleWriteFlush(ch, msg)

    systemImpl.lock.unlock()
    response.future
  }

}
