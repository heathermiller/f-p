package silt

import scala.language.implicitConversions

import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener

object Implicits {

  implicit def funToListener[T](fun: ChannelFuture => T): ChannelFutureListener = new ChannelFutureListener {
    override def operationComplete(future: ChannelFuture): Unit = fun(future)
  }

}
