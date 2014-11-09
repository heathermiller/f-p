package silt
package netty

import scala.pickling._
import Defaults._
// import binary._

import _root_.io.netty.channel.Channel

import scala.concurrent.{Future, Promise}

import graph.Picklers._


trait SendUtils {

  def systemImpl: SystemImpl

  def pickleWriteFlush[T: Pickler](ch: Channel, msg: T): Unit = {
    // PICKLING
    // val arr = msg.pickle.value

    // 1. pickle value
    val tag = implicitly[Pickler[T]].tag
    println(s"tag: ${tag.key}")

    if (msg.isInstanceOf[graph.Graph] || msg.isInstanceOf[ForceResponse]) {
      import json._
      val builder = pickleFormat.createBuilder()
      builder.hintTag(tag)
      val pickler = implicitly[Pickler[T]]
      println(s"pickler class: ${pickler.getClass.getName}")
      pickler.pickle(msg, builder)
      val p = builder.result()
      val arr = p.value
      println(s"result: $arr")
      print(s"trying to unpickle... ")
      val up = p.unpickle[Any]
      println(up)
    }

    import binary._
    val builder = pickleFormat.createBuilder()
    builder.hintTag(tag)
    val pickler = implicitly[Pickler[T]]
    // println(s"pickler class: ${pickler.getClass.getName}")
    pickler.pickle(msg, builder)
    val p = builder.result()
    val arr = p.value

    val buf = ch.alloc().buffer(arr.length)
    buf.writeBytes(arr)
    println(s"writing $msg to channel $ch...")
    // TODO: why do we have to sync here?
    // isn't this a performance issue?
    ch.writeAndFlush(buf).sync()
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
