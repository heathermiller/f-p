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

import silt.{SiloRef, Host, LocalSilo, Emitter, SiloSystem}
import silt.netty.{Server, TestSiloFactory, Person}


object BasicMultiJvmNode1 {
  def main(args: Array[String]): Unit =
    Server(8090).run()
}

object BasicMultiJvmNode2 {
  val numPersons = 10

  def populateSilo(): LocalSilo[Person, List[Person]] = {
    val persons: List[Person] = for (_ <- (1 to numPersons).toList) yield {
      val (randomId, randomAge, randomLoc) = (Random.nextInt(10000000), Random.nextInt(100), Random.nextInt(200))
      new Person(randomId, randomAge, randomLoc)
    }
    new LocalSilo(persons)
  }

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000) // FIXME

    val system = SiloSystem()
    val host = Host("127.0.0.1", 8090)
    val siloFut: Future[SiloRef[Int, List[Int]]] = system.fromClass[Int, List[Int]](classOf[TestSiloFactory], host)

    val siloFut2 = siloFut.flatMap { (silo: SiloRef[Int, List[Int]]) =>
      silo.send()
    }
    val res = Await.result(siloFut2, 5.seconds)
    assert(res.toString == "List(40, 30, 20)")

    val siloFut3 = siloFut.flatMap { (silo: SiloRef[Int, List[Int]]) =>
      // problem: spore unpickler captures enclosing function
      // --> constructur of spore unpickler class takes fun param!
      val silo2 = silo.apply[Int, List[Int]](spore { (l: List[Int]) => l.map(_ + 1) })
      silo2.send()
    }
    val res3 = Await.result(siloFut3, 5.seconds)
    assert(res3.toString == "List(41, 31, 21)")

    // test handling of InitSiloFun
    val sourceFut = system.fromFun(host)(populateSilo)
    Await.ready(sourceFut, 5.seconds)

    testPumpTo(system, sourceFut, host)

    val sourceFut2 = system.fromFun(host)(populateSilo)
    for (_ <- 1 to 10) {
      testPumpTo2(system, sourceFut, sourceFut2, host)
    }

    system.waitUntilAllClosed()
  }

  def testPumpTo(system: SiloSystem, sourceFut: Future[SiloRef[Person, List[Person]]], host: Host): Unit = {
    val fut = sourceFut.flatMap { source =>
      val target = system.emptySilo[Person, List[Person]](host)
      val s = spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) }
      source.pumpTo(target)(s)
      target.send()
    }
    val res = Await.result(fut, 15.seconds).asInstanceOf[List[Person]]
    assert(res.size == 10)
  }

  def testPumpTo2(system: SiloSystem, sourceFut: Future[SiloRef[Person, List[Person]]], sourceFut2: Future[SiloRef[Person, List[Person]]], host: Host): Unit = {
    val fut = sourceFut.zip(sourceFut2).flatMap { case (source1, source2) =>
      val target = system.emptySilo[Person, List[Person]](host)
      val s = spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) }
      source1.pumpTo(target)(s)
      source2.pumpTo(target)(s)
      target.send()
    }
    val res = Await.result(fut, 15.seconds).asInstanceOf[List[Person]]
    assert(res.size == 20)
  }
}
