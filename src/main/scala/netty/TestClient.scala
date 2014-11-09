package silt
package netty

import scala.pickling._
import Defaults._

import scala.spores._
import SporePickler._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


class TestSiloFactory extends SiloFactory[Int, List[Int]] {
  def data = new LocalSilo(List(40, 30, 20))
}

object TestClient extends SendUtils {
  val systemImpl = new SystemImpl

  def main(args: Array[String]): Unit = {
    val system = systemImpl
    val siloFut = system.fromClass[Int, List[Int]](classOf[TestSiloFactory], Host("127.0.0.1", 8090))
    Await.ready(siloFut, 5.seconds)
    system.waitUntilAllClosed()
  }
}

object TestClient2 extends SendUtils {
  val systemImpl = new SystemImpl

  def main(args: Array[String]): Unit = {
    val system = systemImpl
    val siloFut: Future[SiloRef[Int, List[Int]]] = system.fromClass[Int, List[Int]](classOf[TestSiloFactory], Host("127.0.0.1", 8090))
    val siloFut2 = siloFut.flatMap { (silo: SiloRef[Int, List[Int]]) =>
      // problem: spore unpickler captures enclosing function
      // --> constructur of spore unpickler class takes fun param!
      val silo2 = silo.apply[Int, List[Int]](spore { (l: List[Int]) => l.map(_ + 1) })
      silo2.send()
    }
    val res = Await.result(siloFut2, 5.seconds)
    println(res)
    system.waitUntilAllClosed()
  }
}

object TestClient3 extends SendUtils {
  val systemImpl = new SystemImpl

  def main(args: Array[String]): Unit = {
    val system = systemImpl
    val siloFut: Future[SiloRef[Int, List[Int]]] = system.fromClass[Int, List[Int]](classOf[TestSiloFactory], Host("127.0.0.1", 8090))
    val siloFut2 = siloFut.flatMap { (silo: SiloRef[Int, List[Int]]) =>
      silo.send()
    }
    val res = Await.result(siloFut2, 5.seconds)
    println(s"RESULT: $res")
    system.waitUntilAllClosed()
  }
}
