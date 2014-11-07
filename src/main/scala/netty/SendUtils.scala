package silt
package netty

import scala.pickling._
import binary._

import _root_.io.netty.channel.Channel

import scala.concurrent.{Future, Promise}


trait SendUtils {

  def systemImpl: SystemImpl

  def pickleWriteFlush[T: SPickler: FastTypeTag](ch: Channel, msg: T): Unit = {
    // PICKLING
    // val arr = msg.pickle.value

    // 1. pickle value
    val builder = pickleFormat.createBuilder()
    builder.hintTag(implicitly[FastTypeTag[T]])
    implicitly[SPickler[T]].pickle(msg, builder)
    val p = builder.result()
    val arr = p.value

    val buf = ch.alloc().buffer(arr.length)
    buf.writeBytes(arr)
    println(s"writing $msg to channel $ch...")
    // TODO: why do we have to sync here?
    // isn't this a performance issue?
    ch.writeAndFlush(buf).sync()
  }

  def sendToChannel[T: SPickler: FastTypeTag](ch: Channel, msg: T): Unit = {
    // TODO: what is this lock used for?
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
