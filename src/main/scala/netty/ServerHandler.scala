package silt
package netty

import io.netty.buffer.ByteBuf

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener

import io.netty.util.ReferenceCountUtil

import scala.pickling._
import binary._

import Implicits._

import java.io.ByteArrayOutputStream
import java.util.concurrent.CountDownLatch
import akka.actor.{Actor, ActorRef}


case class HandleIncoming(msg: Any, ctx: ChannelHandlerContext)

class Receptor(system: SystemImpl) extends Actor with SendUtils {

  def systemImpl = system

  def receive = {
    case HandleIncoming(msg, ctx) =>
      val in: ByteBuf = msg.asInstanceOf[ByteBuf]

      val bos = new ByteArrayOutputStream

      try {
        while (in.isReadable()) bos.write(in.readByte().asInstanceOf[Int])
      } finally {
        ReferenceCountUtil.release(msg)
      }

      val arr = bos.toByteArray()
      val pickle = BinaryPickle(arr)
      val command = pickle.unpickle[Any]
      println(s"SERVER: received $command")

      command match {
        case Terminate() =>
          println(s"SERVER: closing ${ctx.channel()}")
          ctx.close().sync()
          system.latch.countDown()

        case theMsg @ InitSilo(fqcn, refId) =>
          println(s"SERVER: creating DS using class $fqcn...")
          val clazz = Class.forName(fqcn)
          println(s"SERVER: looked up $clazz")
          val inst = clazz.newInstance()//.asInstanceOf[SiloFactory[Any]]
          println(s"SERVER: created instance $inst")

          inst match {
            case factory: SiloFactory[u, t] =>
              val theDS = factory.data
              system.localSiloRefOf += (refId -> theDS)

              println(s"SERVER: created $theDS. responding...")

              val replyMsg = OKCreated(refId)
              replyMsg.id = theMsg.id
              sendToChannel(ctx.channel(), replyMsg)

            case _ => /* do nothing */
          }

        case theMsg: ApplyMessage[u, a, v, b] =>
          val name    = theMsg.refId
          val fun     = theMsg.fun
          val newName = theMsg.newRefId

          print(s"SERVER: sending function to DS '$name': ")
          // look up DS
          val theDS = system.localSiloRefOf(name)//.asInstanceOf[DS[Any]]
          println(theDS.toString)

          // val oldFun: T => S = fun
          // val newFun: Any => Any = (arg: Any) => {
          //   val typedArg = arg.asInstanceOf[T]
          //   println(s"newFun: typedArg = $typedArg")
          //   println(s"newFun: fun = $fun")
          //   oldFun.apply(typedArg)
          // }

          // newDS is guaranteed to be local
          val newDS = theDS.internalApply[a, v, b](fun)
          println(s"SERVER: value of new DS: ${newDS.value}")
          system.localSiloRefOf += (newName -> newDS)

        case theMsg @ ForceMessage(name) =>
          println(s"SERVER: forcing SiloRef '$name'...")
          // look up SiloRef
          val theDS = system.localSiloRefOf(name)//.asInstanceOf[SiloRef[Any]]

          val replyMsg = ForceResponse(theDS/*.asInstanceOf[LocalSilo[Any]]*/.value)
          replyMsg.id = theMsg.id
          sendToChannel(ctx.channel(), replyMsg)
      }
  }
}

/**
 * Handles a server-side channel.
 */
class ServerHandler(system: SystemImpl, receptor: ActorRef) extends ChannelInboundHandlerAdapter with SendUtils {

  def systemImpl: SystemImpl = system

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("SERVER: enter channelActive")

    // val time: ByteBuf = ctx.alloc().buffer(4)
    // print("writing time...")
    // time.writeInt((System.currentTimeMillis() / 1000L + 2208988800L).asInstanceOf[Int])

    // val f: ChannelFuture = ctx.writeAndFlush(time)
    // println("done")
    // f.addListener((future: ChannelFuture) => assert(f == future))

    sendToChannel(ctx.channel, s"Time: something")
/*
        final ByteBuf time = ctx.alloc().buffer(4)
        time.writeInt((int) (System.currentTimeMillis() / 1000L + 2208988800L))

        final ChannelFuture f = ctx.writeAndFlush(time); // (3)
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                assert f == future;
                ctx.close();
            }
        }); // (4)
*/
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    println("SERVER: enter channelRead")

    receptor.tell(HandleIncoming(msg, ctx), Actor.noSender)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Close the connection when an exception is raised.
    cause.printStackTrace()
    ctx.close()
  }

}
