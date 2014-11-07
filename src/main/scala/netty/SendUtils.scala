package silt
package netty

import scala.pickling._
import binary._

import _root_.io.netty.channel.Channel

import scala.concurrent.{Future, Promise}


trait SendUtils {

  def systemImpl: SystemImpl

  def pickleWriteFlush[T: SPickler: FastTypeTag](ch: Channel, msg: T): Unit = {
    val arr = msg.pickle.value
    val buf = ch.alloc().buffer(arr.length)
    buf.writeBytes(arr)
    println(s"writing $msg to channel $ch...")
    ch.writeAndFlush(buf).sync()
  }

  def sendToChannel[T: SPickler: FastTypeTag](ch: Channel, msg: T): Unit = {
    systemImpl.lock.lock()
    pickleWriteFlush(ch, msg)
    systemImpl.lock.unlock()
  }

  def sendWithReply[T <: ReplyMessage : SPickler : FastTypeTag](ch: Channel, msg: T): Future[Any] = {
    systemImpl.lock.lock()

    val response = Promise[Any]()
    systemImpl.promiseOf += (msg.id -> response)
    pickleWriteFlush(ch, msg)

    systemImpl.lock.unlock()
    response.future
  }

}
