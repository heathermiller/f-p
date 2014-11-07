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

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import scala.util.{Try, Success, Failure}

import Implicits._

import scala.pickling._
import binary._


abstract class ClientHandler extends ChannelInboundHandlerAdapter with SendUtils {
  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    println(s"CLIENT: enter channelRead: ${ctx.channel()}")

    val in: ByteBuf = msg.asInstanceOf[ByteBuf]
    val bos = new ByteArrayOutputStream

    try {
      while (in.isReadable()) {
        //System.out.print(in.readByte().asInstanceOf[Char])
        //System.out.flush()
        bos.write(in.readByte().asInstanceOf[Int])
      }
    } finally {
      ReferenceCountUtil.release(msg)
    }

    val arr = bos.toByteArray()
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

////////////////////////////////


abstract class Status
case class Connected(ch: Channel, group: EventLoopGroup) extends Status
case object Disconnected extends Status

case class Terminate()

trait AgentRef


class MyDSFactory extends SiloFactory[Int, List[Int]] {
  def data = new LocalSilo(List(4, 3, 2))
}


object Client extends SendUtils {

  val system = new SystemImpl
  def systemImpl = system

  def main(args: Array[String]): Unit = {
    // obtain ref to DS based on its data; config says where it's gonna be cached
    val fut = system.fromClass[Int, List[Int]](classOf[MyDSFactory], Host("127.0.0.1", 8090))

    val done = fut.flatMap { ds =>
      val ds2 = ds.apply[String, List[String]](spore { v =>
        v.map(x => s"[$x]")
      })

      val resFut = ds2.send()

      resFut andThen { case Success(s) =>
        println(s"the value is: $s")
      }
    }

    Await.ready(done, 5.seconds)
    system.waitUntilAllClosed()
  }

}
