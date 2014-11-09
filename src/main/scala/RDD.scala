package silt

import scala.spores._
import scala.pickling._
import Defaults._

import scala.reflect.ClassTag

import scala.collection.immutable.Seq
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import netty.SystemImpl


trait RDD[T] {
  def map[S](fun: T => S): RDD[S]
  def takeAllAsync: Future[Seq[T]]
}

class RDDImpl[T](dss: Seq[SiloRef[T, Seq[T]]]) extends RDD[T] {
  def map[S](fun: T => S): RDD[S] = {
    val mappeddss = dss map { (ds: SiloRef[T, Seq[T]]) =>
      ds.apply[S, Seq[S]](spore {
        val localFun = fun
        x => x.map(localFun)
      })
    }
    new RDDImpl(mappeddss)
  }
  def takeAllAsync: Future[Seq[T]] = {
    val futs: Seq[Future[Seq[T]]] = dss.map(_.send())
    Future.sequence(futs).map(_.flatten)
  }
}

class SeqDSFactory extends SiloFactory[Int, Seq[Int]] {
  def data = new LocalSilo(Seq(4, 3, 2))
}

object RDDDemo extends App {

  val system = new SystemImpl

  val dsfut1 = system.fromClass[Int, Seq[Int]](classOf[SeqDSFactory], Host("127.0.0.1", 8090))
  val dsfut2 = system.fromClass[Int, Seq[Int]](classOf[SeqDSFactory], Host("127.0.0.1", 8091))

  val data1 = Seq(1, 2, 3, 4)
  val data2 = Seq(5, 6, 7, 8)

  val res = dsfut1.zip(dsfut2) flatMap { case (preds1, preds2) =>
    val ds1 = preds1.apply[Int, Seq[Int]]({ x =>
      println(s"fun1: x = $x")
      println(s"fun1: returning $data1")
      data1
    })
    val ds2 = preds2.apply[Int, Seq[Int]]({ x => data2 })

    Thread.sleep(2000)

    val rdd: RDD[Int] = new RDDImpl[Int](Seq(ds1, ds2))
    val rdd2 = rdd.map(x => x * 2)

    Thread.sleep(2000)

    rdd2.takeAllAsync
  }

  val s = Await.result(res, 5.seconds).mkString("{{", ",", "}}")
  println(s"the result is: $s")

  system.waitUntilAllClosed()
}
