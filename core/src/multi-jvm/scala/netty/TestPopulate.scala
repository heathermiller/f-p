package netty

import scala.pickling._
import Defaults._
import shareNothing._

import scala.spores._
import SporePickler._

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random

import silt.{SiloRef, Host, LocalSilo, Emitter, SiloSystem, SiloFactory}
import silt.netty.Server


object TestPopulateMultiJvmNode1 {
  def main(args: Array[String]): Unit =
    Server(8090).run()
}

object TestPopulateMultiJvmNode2 {
  def main(args: Array[String]): Unit = {
    Thread.sleep(1000) // FIXME

    implicit val system = SiloSystem("silt.netty.SystemImpl")
    val host = Host("127.0.0.1", 8090)
    val silo: SiloRef[List[Int]] =
      SiloRef.populate(host, List(40, 30, 20))

    val siloFut2 = silo.send()

    val res = Await.result(siloFut2, 5.seconds)
    assert(res.toString == "List(40, 30, 20)")

    system.waitUntilAllClosed()
  }
}
