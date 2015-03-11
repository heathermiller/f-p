package silt
package netty

import scala.pickling._
import Defaults._
import shareNothing._

import scala.spores._
import SporePickler._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

import silt.{SiloRef, SiloFactory, Host, LocalSilo}


case class Person(id: Int, age: Int, location: Int)


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


object TestClient4 extends App {
  val numPersons = 10

  def populateSilo(): LocalSilo[Person, List[Person]] = {
    val persons: List[Person] = for (_ <- (1 to numPersons).toList) yield {
      val (randomId, randomAge, randomLoc) = (Random.nextInt(10000000), Random.nextInt(100), Random.nextInt(200))
      new Person(randomId, randomAge, randomLoc)
    }
    new LocalSilo(persons)
  }

  val system = new SystemImpl // create Silo system
  Await.ready(system.start(), 1.seconds)

  // val hosts = for (port <- List(8090, 8091, 8092, 8093)) yield Host("127.0.0.1", port)
  val host = Host("127.0.0.1", 8090)

  val sourceFut = system.fromFun(host)(populateSilo)

  val fut = sourceFut.foreach { source =>
    println("YAY")
    // val target = system.emptySilo[Person, List[Person]](host)
    // source.pumpTo(target)(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
    // target.send()
  }

  Await.ready(sourceFut, 5.seconds)

  // shutdown Silo system
  system.waitUntilAllClosed()
}
