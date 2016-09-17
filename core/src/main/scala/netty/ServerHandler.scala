package silt
package netty

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

import scala.pickling._
import shareNothing._
import Defaults._

import scala.concurrent.Promise

import java.util.concurrent.BlockingQueue


case class HandleIncoming(msg: Any, ctx: ChannelHandlerContext)
class HandleIncomingLocal(msg: Any, ctx: ChannelHandlerContext, val resultPromise: Promise[Option[Any]]) extends HandleIncoming(msg, ctx)


/**
 * Handles a server-side channel.
 */
class ServerHandler(system: SystemImpl, queue: BlockingQueue[HandleIncoming]) extends ChannelInboundHandlerAdapter with SendUtils {

  def systemImpl: SystemImpl = system

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    // println("SERVER: enter channelActive")
    sendToChannel(ctx.channel, s"Time: something")
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    // println("SERVER: enter channelRead")
    queue.add(HandleIncoming(msg, ctx))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Close the connection when an exception is raised.
    cause.printStackTrace()
    ctx.close()
  }

}
