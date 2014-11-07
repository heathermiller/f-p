package silt
package actors
package test

import scala.spores._
import scala.pickling._
import binary._

import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


case class Item(s: String)

object ActorsBackendTest {
  implicit val itemUnpickler = implicitly[Unpickler[Item]]
}

class ActorsBackendTest {
  import ActorsBackendTest._

  @Test
  def applyAndSend(): Unit = {
    val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val fut = system.fromClass[Int, List[Int]](classOf[MySiloFactory], host)

    val done1 = fut.flatMap { siloref =>
      val siloref2 = siloref.apply[String, List[String]](spore { data =>
        data.map(x => s"[$x]")
      })

      siloref2.send()
    }

    val res1 = Await.result(done1, 5.seconds)
    system.waitUntilAllClosed()
    println(s"result 1: $res1")
    assert(res1.toString == "List([4], [3], [2])")
  }

  @Test
  def pumpTo(): Unit = {
    val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val fut = system.fromClass[Int, List[Int]](classOf[MySiloFactory], host)

    val done2 = fut.flatMap { siloRef =>
      val dest = Host("127.0.0.1", 8091)
      val destSilo = system.emptySilo[Item, List[Item]](dest)

      siloRef.pumpTo(destSilo)(spore { (elem: Int, emit: Emitter[Item]) =>
        val i = Item((elem + 10).toString)
        emit.emit(i)
        emit.emit(i)
      })

      destSilo.send()
    }

    val res2 = Await.result(done2, 5.seconds)
    system.waitUntilAllClosed()
    println(s"result 2: $res2")
    assert(res2 == List(Item("14"), Item("14"), Item("13"), Item("13"), Item("12"), Item("12")))
  }
}
