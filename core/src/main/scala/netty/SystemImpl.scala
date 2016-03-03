package silt
package netty

import scala.reflect.{ClassTag, classTag}

import scala.pickling._
import Defaults._
import runtime.GlobalRegistry
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
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}
import scala.collection.mutable
import scala.collection.concurrent.TrieMap

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

import graph.Picklers._


// needs a way to talk to servers on other nodes
// mapping from port to Bootstrap?
class SystemImpl extends SiloSystem with SiloSystemInternal with SendUtils {
  self =>

  def systemImpl = self

  // connection status
  val statusOf: mutable.Map[Host, ConnectionStatus] = new TrieMap[Host, ConnectionStatus]

  // replies
  val promiseOf: mutable.Map[Int, Promise[Any]] = new TrieMap[Int, Promise[Any]]

  // SiloRef map
  val localSiloRefOf: mutable.Map[Int, LocalSilo[_, _]] = new TrieMap[Int, LocalSilo[_, _]]

  val lock = new ReentrantLock

  val latch = new CountDownLatch(1)

  def register[T: ClassTag: Pickler: Unpickler](): Unit = {
    val clazz = classTag[T].runtimeClass
    val p = implicitly[Pickler[T]]
    val up = implicitly[Unpickler[T]]
    // println(s"registering for ${clazz.getName()} pickler of class type ${p.getClass.getName}...")
    GlobalRegistry.picklerMap += (clazz.getName() -> (x => p))
    GlobalRegistry.unpicklerMap += (clazz.getName() -> up)
  }

  register[graph.Graph]
  register[graph.CommandEnvelope]

  def start(): Future[Boolean] = Future.successful(true)

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

  def waitUntilAllClosed(tm1: FiniteDuration, tm2: FiniteDuration): Unit = {
    waitUntilAllClosed()
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

  def talkTo(host: Host): Future[Channel] = {
    val futChannel = statusOf.get(host) match {
      case None =>
        val chFut = initChannel(host.address, host.port)
        val p = Promise[Channel]()
        chFut.onComplete {
          case Success(conn @ Connected(channel, _)) =>
            statusOf += (host -> conn)
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

  // effects: refIds, seqNum, location
  def initRequest[U, T <: Traversable[U], V <: ReplyMessage : Pickler](host: Host, mkMsg: Int => V): Future[SiloRef[U, T]] = {
    val refId = refIds.incrementAndGet()
    val msg   = mkMsg(refId)
    msg.id    = seqNum.incrementAndGet()
    location += (refId -> host)
    println(s"initRequest: connecting to $host...")
    send(host, msg).map { x =>
      println("SystemImpl: got response for InitSilo msg")
      // create a typed wrapper
      new graph.MaterializedSiloRef[U, T](refId, host)(this)
    }
  }

  def send[T <: ReplyMessage : Pickler](host: Host, msg: T): Future[Any] =
    talkTo(host).flatMap(channel => sendWithReply(channel, msg).map {
      case ForceResponse(v) => v
      case msg => msg
    })
}
