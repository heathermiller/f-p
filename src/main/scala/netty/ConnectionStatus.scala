package silt
package netty

import io.netty.channel.{Channel, EventLoopGroup}


abstract class ConnectionStatus
case class Connected(ch: Channel, group: EventLoopGroup) extends ConnectionStatus
case object Disconnected extends ConnectionStatus
