package baby_spark

import scala.language.higherKinds

import scala.collection.generic._

import scala.spores._
import scala.pickling._
import Defaults._

import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration.Duration

import SporePickler._

import scala.io.Source

import silt._

object RDD {
  def apply[T, Container[A] <: Traversable[A]](silos: SiloRef[T, Container[T]]): RDD[T, Container] =
    new RDD(List(silos))


  def apply[T, Container[A] <: Traversable[A]](silos: Seq[SiloRef[T, Container[T]]]): RDD[T, Container] =
    new RDD(silos)

  // Future isn't needed if instead the creation of a Silo returns a SiloRef directly
  def fromTextFile(filename: String)(implicit system: SiloSystem, host: Host, ec: ExecutionContext): Future[RDD[String, List]] = {
    system.fromFun(host)(spore {
      val fl = filename
      _: Unit => {
        val lines = Source.fromFile(fl).mkString.split('\n').toList
        new LocalSilo[String, List[String]](lines)
      }
    }).map { RDD(_) }
  }
}

class RDD[T, Container[A] <: Traversable[A]] private (val silos: Seq[SiloRef[T, Container[T]]]) {

  // Trick from https://stackoverflow.com/questions/3225675/can-i-pimp-my-library-with-an-analogue-of-traversablelike-map-that-has-nicely
  // Note
  type CanBuildTo[Elem, C[X]] = CanBuildFrom[Nothing, Elem, C[Elem]]

  def map[B](f: Spore[T, B])(implicit cbt: CanBuildTo[B, Container]): RDD[B, Container] = {
    RDD(silos.map {
      s => s.apply[B, Container[B]](spore {
        val localFunc = f
        implicit val lCbt = cbt
        content => {
          content.map[B, Container[B]](localFunc)(collection.breakOut(lCbt))
        }
      })
    })
  }

  def filter(f: Spore[T, Boolean]): RDD[T, Container] = ???

  def flatMap[B](f: Spore[T, Container[B]])(implicit cbf: CanBuildTo[B, Container]): RDD[B, Container] = {
    val resList = silos.map {
      s => s.apply[B, Container[B]](spore {
        val func = f
        implicit val lCbf = cbf
        content => {
          content.flatMap[B, Container[B]](func)(collection.breakOut(lCbf))
        }
      })
    }
    RDD(resList)
  }

  def reduce(f: Spore2[T, T, T])(implicit ec: ExecutionContext): T = {
    val resList = silos.map {
      s => s.apply[T, List[T]](spore {
        val reducer = f
        content => {
          content.reduce(reducer) :: Nil
        }
      }).send()
    }
    val res: Future[Seq[List[T]]] = Future.sequence(resList)
    val res1 = Await.result(res, Duration.Inf).flatten
    res1.reduce(f)
  }

  def collect()(implicit ec: ExecutionContext): Seq[T] = {
    Await.result(Future.sequence(silos.map {
      s => s.send()
    }), Duration.Inf).flatten
  }

}

object RDDExample {

  import silt.actors._
  import scala.concurrent._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]): Unit = {
    implicit val system = new SystemImpl
    val started = system.start()
    implicit val host = Host("127.0.0.1", 8090)

    Await.ready(started, 1.seconds)

    val content = Await.result(RDD.fromTextFile("data/data.txt"), Duration.Inf)

    val words = content.flatMap(line => line.split(' ').toList)
    println(words.collect())

    val lineLength = content.map(line => line.length)
    val res = lineLength.reduce(spore {
      (a, b) => a + b
    })
    println(res)

    system.waitUntilAllClosed()
  }
}
