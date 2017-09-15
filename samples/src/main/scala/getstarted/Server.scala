package samples
package getstarted 

import silt.netty.{Server => NettyServer}

object Server {

  def main(args: Array[String]): Unit =
    NettyServer(8090).run()
}
