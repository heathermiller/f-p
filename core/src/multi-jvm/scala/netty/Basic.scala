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


object BasicMultiJvmNode1 {
  def main(args: Array[String]): Unit =
    Server(8090).run()
}

case class Person(id: Int, age: Int, location: Int)

class TestSiloFactory extends SiloFactory[List[Int]] {
  def data = new LocalSilo(List(40, 30, 20))
}

object BasicMultiJvmNode2 {
  val numPersons = 10

  def populateSilo(): LocalSilo[List[Person]] = {
    val persons: List[Person] = for (_ <- (1 to numPersons).toList) yield {
      val (randomId, randomAge, randomLoc) = (Random.nextInt(10000000), Random.nextInt(100), Random.nextInt(200))
      new Person(randomId, randomAge, randomLoc)
    }
    new LocalSilo(persons)
  }

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000) // FIXME

    implicit val system = SiloSystem()
    val host = Host("127.0.0.1", 8090)
    val siloFut: Future[SiloRef[List[Int]]] = SiloRef.fromClass[List[Int]](classOf[TestSiloFactory], host)

    val siloFut2 = siloFut.flatMap { (silo: SiloRef[List[Int]]) =>
      silo.send()
    }
    val res = Await.result(siloFut2, 5.seconds)
    assert(res.toString == "List(40, 30, 20)")

    val siloFut3 = siloFut.flatMap { (silo: SiloRef[List[Int]]) =>
      // problem: spore unpickler captures enclosing function
      // --> constructur of spore unpickler class takes fun param!
      val silo2 = silo.apply[List[Int]](spore { (l: List[Int]) => l.map(_ + 1) })
      silo2.send()
    }
    val res3 = Await.result(siloFut3, 5.seconds)
    assert(res3.toString == "List(41, 31, 21)")

    // test handling of InitSiloFun
    val sourceFut = SiloRef.fromFun(host)(spore { (x: Unit) => populateSilo() })
    Await.ready(sourceFut, 5.seconds)

    testPumpTo(system, sourceFut, host)

    val sourceFut2 = SiloRef.fromFun(host)(spore { (x: Unit) => populateSilo() })
    for (_ <- 1 to 10) {
      testPumpTo2(system, sourceFut, sourceFut2, host)
    }

    system.waitUntilAllClosed()
  }

  def testPumpTo(system: SiloSystem, sourceFut: Future[SiloRef[List[Person]]], host: Host): Unit = {
    val fut = sourceFut.flatMap { source =>
      val target = system.emptySilo[List[Person]](host)
      val s = spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) }
      source.elems[Person].pumpTo[Person,List[Person],Spore2[Person,Emitter[Person],Unit]](target)(s)
      target.send()
    }
    val res = Await.result(fut, 15.seconds).asInstanceOf[List[Person]]
    assert(res.size == 10)
  }

  def testPumpTo2(system: SiloSystem, sourceFut: Future[SiloRef[List[Person]]], sourceFut2: Future[SiloRef[List[Person]]], host: Host): Unit = {
    val fut = sourceFut.zip(sourceFut2).flatMap { case (source1, source2) =>
      val target = system.emptySilo[List[Person]](host)
      val s = spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) }
      source1.elems[Person].pumpTo[Person,List[Person],Spore2[Person,Emitter[Person],Unit]](target)(s)
      source2.elems[Person].pumpTo[Person,List[Person],Spore2[Person,Emitter[Person],Unit]](target)(s)
      target.send()
    }
    val res = Await.result(fut, 15.seconds).asInstanceOf[List[Person]]
    assert(res.size == 20)
  }
}
