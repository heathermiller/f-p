package silt
package netty

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{EventLoopGroup, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}


class Server(port: Int, system: SystemImpl) {

  //FIXME: hostname
  private val host = Host("127.0.0.1", port)

  def run(): Unit = {
    println(s"SERVER [$host]: starting...")

    val bossGroup: EventLoopGroup = new NioEventLoopGroup
    val workerGroup: EventLoopGroup = new NioEventLoopGroup

    // Queue for transferring messages from the netty event loop to the server's event loop
    val queue: BlockingQueue[HandleIncoming] = new LinkedBlockingQueue[HandleIncoming]()
    // Thread that blocks on the queue
    val receptorRunnable = new ReceptorRunnable(queue, system, host)
    val receptorThread = new Thread(receptorRunnable)
    receptorThread.start()

    try {
      val b = new ServerBootstrap
      b.group(bossGroup, workerGroup)
       .channel(classOf[NioServerSocketChannel])
       .childHandler(new ChannelInitializer[SocketChannel]() {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(new ServerHandler(system, queue))
          }
       })
       .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128)
       .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

      // Bind port and start accepting incoming connections
      b.bind(port).sync()

      system.latch.await()
      print(s"SERVER [$host]: shutting down...")

      receptorRunnable.shouldTerminate = true
      receptorThread.interrupt()
      receptorThread.join()

      println("DONE")
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

object Server {

  val NO_CHUNK: Byte = 0
  val FST_CHUNK: Byte = 1
  val MID_CHUNK: Byte = 2
  val LAST_CHUNK: Byte = 3

  scala.pickling.runtime.GlobalRegistry.picklerMap += ("silt.graph.CommandEnvelope" -> { x => silt.graph.Picklers.CommandEnvelopePU })
  scala.pickling.runtime.GlobalRegistry.unpicklerMap += ("silt.graph.CommandEnvelope" -> silt.graph.Picklers.CommandEnvelopePU)

  def main(args: Array[String]): Unit = {
    val port =
      if (args.length > 0) Integer.parseInt(args(0))
      else 8080

    val system = new SystemImpl

    new Server(port, system).run()
  }

}
