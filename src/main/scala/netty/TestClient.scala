package silt
package netty

import scala.concurrent.Await
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
