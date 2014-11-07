package silt
package actors

import scala.spores._

import scala.pickling._
import binary._

import scala.util.{Try, Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


class MySiloFactory extends SiloFactory[Int, List[Int]] {

  def data: LocalSilo[Int, List[Int]] =
    new LocalSilo(List(4, 3, 2))

}

class MyPairFactory extends SiloFactory[(Int, String), List[(Int, String)]] {
  def data: LocalSilo[(Int, String), List[(Int, String)]] =
    new LocalSilo(List((4, "a"), (3, "b"), (2, "c")))
}

class MyPairFactory2 extends SiloFactory[(Int, String), List[(Int, String)]] {
  def data: LocalSilo[(Int, String), List[(Int, String)]] =
    new LocalSilo(List((4, "e"), (3, "r"), (2, "t")))
}

object DemoClient {
  val system = new SystemImpl
  def systemImpl = system

  val unpickler = implicitly[Unpickler[(Int, List[String])]]

  def main(args: Array[String]): Unit = {
    val host = Host("127.0.0.1", 8090)

/*
    val fut = system.fromClass[List[Int]](classOf[MySiloFactory], host)

    ////////////////////////////////////////////////////////////////
    // REDUCE:

    val done3 = fut.flatMap { silo =>
      // duplicate `silo` onto a different node
      val dest = Host("127.0.0.1", 8091)
      val silo2 = system.emptySilo[List[Int]](dest)
      silo.pumpTo(silo2) { (elem: Int, emit: Emitter[Int]) => emit.emit(elem) }

      // reduce
      val reduced1: SiloRef[List[Int]] = silo.apply(list => List(list.reduce(_ + _)))
      val reduced2: SiloRef[List[Int]] = silo2.apply(list => List(list.reduce(_ + _)))

      val merged = system.emptySilo[List[Int]](dest)
      reduced1.pumpTo(merged) { (elem: Int, emit: Emitter[Int]) => emit.emit(elem) }
      reduced2.pumpTo(merged) { (elem: Int, emit: Emitter[Int]) => emit.emit(elem) }
      val resSilo = merged.apply(list => List(list.reduce(_ + _)))

      println(s"done building graph, now sending...")
      resSilo.send()
    }

    val res3 = Await.result(done3, 5.seconds)
    println(s"result 3: $res3")
*/
    ////////////////////////////////////////////////////////////////

    val host2 = Host("127.0.0.1", 8091)
    val fut1 = system.fromClass[(Int, String), List[(Int, String)]](classOf[MyPairFactory], host)
    val fut2 = system.fromClass[(Int, String), List[(Int, String)]](classOf[MyPairFactory2], host2)

    val done4 = fut1.zip(fut2).flatMap { case (silo1, silo2) =>
      val mapped1: SiloRef[(Int, List[String]), Map[Int, List[String]]] = silo1.apply(spore {
        x =>
          val grouped = x.groupBy(p => p._1 % 2)
          grouped.mapValues(list => list.map(_._2))
      })
      val mapped2: SiloRef[(Int, List[String]), Map[Int, List[String]]] = silo2.apply(spore {
        x =>
          val grouped = x.groupBy(p => p._1 % 2)
          grouped.mapValues(list => list.map(_._2))
      })
      // mapped1.send().zip(mapped2.send())

      // on each node, create 2 silos
      // node 1:
      val group11 = system.emptySilo[(Int, List[String]), List[(Int, List[String])]](host)
      val group12 = system.emptySilo[(Int, List[String]), List[(Int, List[String])]](host)
      mapped1.pumpTo(group11)(spore {
        implicit val localUnpickler = unpickler
        (elem: (Int, List[String]), emit: Emitter[(Int, List[String])]) => if (elem._1 == 0) emit.emit(elem)
      })
      mapped1.pumpTo(group12)(spore {
        implicit val localUnpickler = unpickler
        (elem: (Int, List[String]), emit: Emitter[(Int, List[String])]) => if (elem._1 == 1) emit.emit(elem)
      })
      // group11.send()

      // node 2:
      val group21 = system.emptySilo[(Int, List[String]), List[(Int, List[String])]](host2)
      val group22 = system.emptySilo[(Int, List[String]), List[(Int, List[String])]](host2)
      mapped2.pumpTo(group21)(spore {
        implicit val localUnpickler = unpickler
        (elem: (Int, List[String]), emit: Emitter[(Int, List[String])]) => if (elem._1 == 0) emit.emit(elem)
      })
      mapped2.pumpTo(group22)( spore {
        implicit val localUnpickler = unpickler
        (elem: (Int, List[String]), emit: Emitter[(Int, List[String])]) => if (elem._1 == 1) emit.emit(elem)
      })

      // merge group11 and group21
      val group1 = system.emptySilo[(Int, List[String]), List[(Int, List[String])]](host)
      group11.pumpTo(group1)(spore {
        implicit val localUnpickler = unpickler
        (elem, emit) => if (elem._1 == 0) emit.emit(elem)
      })
      group21.pumpTo(group1)(spore {
        implicit val localUnpickler = unpickler
        (elem, emit) => if (elem._1 == 0) emit.emit(elem)
      })

      // merge group12 and group22
      val group2 = system.emptySilo[(Int, List[String]), List[(Int, List[String])]](host2)
      group12.pumpTo(group2)(spore {
        implicit val localUnpickler = unpickler
        (elem, emit) => if (elem._1 == 1) emit.emit(elem)
      })
      group22.pumpTo(group2)(spore {
        implicit val localUnpickler = unpickler
        (elem, emit) => if (elem._1 == 1) emit.emit(elem)
      })

      group1.send().zip(group2.send())
    }

    val res4 = Await.result(done4, 5.seconds)
    println(s"result 4: $res4")

    system.waitUntilAllClosed()
  }
}
