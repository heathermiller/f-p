package silt
package netty

import scala.pickling._
import binary._

import _root_.io.netty.bootstrap.Bootstrap
import _root_.io.netty.channel.Channel
import _root_.io.netty.channel.ChannelInitializer
import _root_.io.netty.channel.ChannelOption
import _root_.io.netty.channel.ChannelFuture
import _root_.io.netty.channel.EventLoopGroup
import _root_.io.netty.channel.nio.NioEventLoopGroup
import _root_.io.netty.channel.socket.SocketChannel
import _root_.io.netty.channel.socket.nio.NioSocketChannel

import Implicits._

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import scala.collection.mutable
import scala.collection.concurrent.TrieMap

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock


// needs a way to talk to servers on other nodes
// mapping from port to Bootstrap?
class SystemImpl extends SiloSystem with SendUtils {
  self =>

  def systemImpl = self

  // connection status
  val statusOf: mutable.Map[Address, Status] = new TrieMap[Address, Status]

  // replies
  val promiseOf: mutable.Map[Int, Promise[Any]] = new TrieMap[Int, Promise[Any]]

  // SiloRef map
  val localSiloRefOf: mutable.Map[Int, LocalSilo[_, _]] = new TrieMap[Int, LocalSilo[_, _]]

  val seqNum = new AtomicInteger(10)
  val refIds = new AtomicInteger(0)
  val emitterIds = new AtomicInteger(0)

  val lock = new ReentrantLock

  val latch = new CountDownLatch(1)

  def initChannel(host: String, port: Int): Future[Connected] = {
    println(s"init channel to port $port...")

    val workerGroup: EventLoopGroup = new NioEventLoopGroup
    try {
      val b = new Bootstrap
      b.group(workerGroup)
      b.channel(classOf[NioSocketChannel])
      b.option(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)
      b.handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline().addLast(new ClientHandler { def systemImpl = self })
        }
      })

      val p = Promise[Connected]()
      val channelFut = b.connect(host, port)
      channelFut.addListener((f: ChannelFuture) => p.success(Connected(f.channel(), workerGroup)))
      p.future
    } catch {
      case t: Throwable =>
        workerGroup.shutdownGracefully()
        throw t
    }
  }

  // client method
  def waitUntilAllClosed(): Unit = {
    for ((a, s) <- statusOf) {
      s match {
        case Connected(ch, workerGroup) =>
          println(s"CLIENT: sending 'Terminate' to $ch...")
          sendToChannel(ch, Terminate())
          print(s"CLIENT: waiting for channel to be closed...")
          ch.closeFuture().sync()
          println("DONE")
          print("shutting down EventLoopGroup...")
          workerGroup.shutdownGracefully()
          println("DONE")

        case _ =>
          /* do nothing */
      }
    }
  }

  def talkTo(host: String, port: Int): Future[Channel] = {
    val a = Address(host, port)
    val futChannel = statusOf.get(a) match {
      case None =>
        val chFut = initChannel(host, port)
        val p = Promise[Channel]()
        chFut.onComplete {
          case Success(conn @ Connected(channel, _)) =>
            statusOf += (a -> conn)
            p.success(channel)
          case Failure(t) =>
            p.failure(t)
        }
        p.future
      case Some(Connected(channel, _)) =>
        println("channel already connected")
        Future.successful(channel)
      case Some(other) =>
        Future.failed(new Exception("unexpected state of channel"))
    }

    futChannel.andThen { case Success(ch) =>
      println(s"channel: $ch")
      println(s"isOpen: ${ch.isOpen}")
      println(s"isWritable: ${ch.isWritable}")
    }
  }

  def fromClass[U, T <: Traversable[U]](clazz: Class[_], h: Host): Future[SiloRef[U, T]] = {
    val port  = h.port
    val host  = h.host
    val refId = refIds.incrementAndGet() - 1
    println(s"fromClass: connecting to $host:$port...")
    val futChannel = talkTo(host, port)

    futChannel.flatMap { channel =>
      val msg = InitSilo(clazz.getName(), refId)
      msg.id = seqNum.incrementAndGet()
      sendWithReply(channel, msg)
    }.map { x =>
      // create a typed wrapper
      new RemoteSiloRef(SiloRefId(refId), self)
    }
  }

}
