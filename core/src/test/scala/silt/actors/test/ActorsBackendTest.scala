package silt
package actors
package test

import scala.spores._
import scala.pickling._
import Defaults._
import binary._

import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


class MySiloFactory extends SiloFactory[List[Int]] {

  def data: LocalSilo[List[Int]] =
    new LocalSilo(List(4, 3, 2))

}

case class Item(s: String)

object ActorsBackendTest {
  implicit val itemUnpickler = implicitly[Unpickler[Item]]
}

class ActorsBackendTest {
  import ActorsBackendTest._

  @Test
  def testApplyAndSend(): Unit = {
    implicit val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val fut = SiloRef.fromClass[List[Int]](classOf[MySiloFactory], host)

    val done1 = fut.flatMap { siloref =>
      val siloref2 = siloref.map[List[String]](spore { data =>
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
  def testPopulate(): Unit = {
    implicit val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val ref = SiloRef.populate(host, Item("hello"))
    val fut = ref.send()
    val res = Await.result(fut, 5.seconds)
    system.waitUntilAllClosed()
    println(s"result: $res")
    assert(res.toString == "Item(hello)")
  }

  @Test
  def testFromTextFile(): Unit = {
    implicit val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val ref = SiloRef.fromTextFile(host, new java.io.File("test-file.txt"))
    val fut = ref.send()
    val res = Await.result(fut, 5.seconds)
    system.waitUntilAllClosed()
    assert(res.toString == "List(hello)")
  }

  //@Test
  def pumpTo(): Unit = {
    implicit val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val fut = SiloRef.fromClass[List[Int]](classOf[MySiloFactory], host)

    val done2 = fut.flatMap { siloRef =>
      val dest = Host("127.0.0.1", 8091)
      val destSilo = system.emptySilo[List[Item]](dest)

      siloRef.elems[Int].pumpTo[Item, List[Item], Spore2[Int,Emitter[Item],Unit]](destSilo)(spore { (elem: Int, emit: Emitter[Item]) =>
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

  @Test
  def testApply(): Unit = {
    implicit val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val fut1 = SiloRef.fromClass[List[Int]](classOf[MySiloFactory], host)
    val fut2 = SiloRef.fromClass[List[Int]](classOf[MySiloFactory], host)

    val done1 = fut1.flatMap { siloref1 =>
      fut2.flatMap { siloref2 =>
        val siloref3 = siloref1.apply[List[Int]](spore {
          val localSiloRef = siloref2
          (data: List[Int]) =>
            localSiloRef.map[List[Int]](spore {
              val localData = data
              (data2: List[Int]) =>
                localData ++ data2
            })
          })
        siloref3.send()
      }
    }

    val res1 = Await.result(done1, 5.seconds)
    system.waitUntilAllClosed()
    println(s"result 1: $res1")
    assert(res1.toString == "List(4, 3, 2, 4, 3, 2)")
  }

  @Test
  def testCurrentHost(): Unit = {
    implicit val system = new SystemImpl
    val host = Host("127.0.0.1", 8090)
    val fut = SiloRef.fromClass[List[Int]](classOf[MySiloFactory], host)

    val done = fut.flatMap { siloref =>
      val siloref2 = siloref.apply[List[Int]](new Spore[List[Int], SiloRef[List[Int]]] {
        def apply(data: List[Int]) =
          SiloRef.populate(SiloRef.currentHost, data.map(x => x + 1))
      })
      siloref2.send()
    }

    val res = Await.result(done, 5.seconds)
    system.waitUntilAllClosed()
    println(s"result: $res")
    assert(res.toString == "List(5, 4, 3)")
  }
}
