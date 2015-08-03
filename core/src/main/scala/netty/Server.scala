package silt
package netty

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{EventLoopGroup, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.{ Logger, StrictLogging => Logging } 

class Server(hostname: String, port: Int, system: SystemImpl) extends AnyRef with Logging {

  private val host = Host(hostname, port)

  /** Initialize and start a Netty-based server as described at [[http://netty.io/wiki/user-guide-for-4.x.html]] */
  def run(): Unit = {
    logger.info(s"SERVER [$host]: starting...")

    val bossGroup: EventLoopGroup = new NioEventLoopGroup
    val workerGroup: EventLoopGroup = new NioEventLoopGroup

    // transfers messages from Netty's event loop to the server's event loop
    val queue: BlockingQueue[HandleIncoming] = new LinkedBlockingQueue[HandleIncoming]()

    // thread that blocks on the queue
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

      // bind port and start accepting incoming connections
      b.bind(port).sync()

      system.latch.await()
      logger.info(s"SERVER [$host]: shutting down...")

      receptorRunnable.shouldTerminate = true
      receptorThread.interrupt()
      receptorThread.join()

      logger.info("DONE")
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

  def main(args: Array[String]): Unit = {
    val port =
      if (args.length > 0) Integer.parseInt(args(0))
      else 8080
    apply(port).run()
  }

  def apply(port: Int): Server =
    new Server("127.0.0.1", port, new SystemImpl)
}
