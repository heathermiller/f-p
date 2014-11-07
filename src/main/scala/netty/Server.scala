package silt
package netty

import io.netty.bootstrap.ServerBootstrap

import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

import scala.collection.mutable
import scala.collection.concurrent.TrieMap

import akka.actor.{ActorSystem, Props}


// for now, every server has hard-coded config
object Config {

  // map node ids to port nums
  val m: mutable.Map[Int, Int] = {
    val trie = new TrieMap[Int, Int]
    trie += (0 -> 8090)
    trie += (1 -> 8091)
    trie += (2 -> 8092)
    trie += (3 -> 8093)
    trie
  }

}


class Server(port: Int, system: SystemImpl) {

  val actorSys = ActorSystem("server-system")

  def run(): Unit = {
    println("starting server...")

    // create an actor for queuing incoming messages
    val receptor = actorSys.actorOf(Props(new Receptor(system)), name = "receptor")

    val bossGroup: EventLoopGroup = new NioEventLoopGroup
    val workerGroup: EventLoopGroup = new NioEventLoopGroup

    try {
      val b = new ServerBootstrap
      val handler = new ServerHandler(system, receptor)
      b.group(bossGroup, workerGroup)
       .channel(classOf[NioServerSocketChannel])
       .childHandler(new ChannelInitializer[SocketChannel]() {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(handler)
          }
       })
       .option(ChannelOption.SO_BACKLOG.asInstanceOf[ChannelOption[Any]], 128)
       .childOption(ChannelOption.SO_KEEPALIVE.asInstanceOf[ChannelOption[Any]], true)

      // Bind and start to accept incoming connections.
      b.bind(port).sync()

      system.latch.await()

      print(s"SERVER: shutting down...")
      actorSys.shutdown()
      println("DONE")
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

object Server {

  def main(args: Array[String]): Unit = {
    val port =
      if (args.length > 0) Integer.parseInt(args(0))
      else 8080

    val system = new SystemImpl

    new Server(port, system).run()
  }

}
