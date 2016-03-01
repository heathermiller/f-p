package baby_spark
package rdd

import scala.language.implicitConversions

import scala.collection.immutable.TreeMap
import scala.collection.generic._

import scala.spores._
import scala.pickling._
import Defaults._

import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration.Duration

import SporePickler._

import scala.io.Source

import scalaz._
import Scalaz._

import silt._

object RDD {
  def apply[T, S <: Traversable[T]](silos: SiloRef[T, S]): RDD[T, S] =
    new RDD(List(silos))


  def apply[T, S <: Traversable[T]](silos: Seq[SiloRef[T, S]]): RDD[T, S] =
    new RDD(silos)

  // Future isn't needed if instead the creation of a Silo returns a SiloRef directly
  def fromTextFile(filename: String)(implicit system: SiloSystem, host: Host, ec: ExecutionContext): Future[RDD[String, List[String]]] = {
    system.fromFun(host)(spore {
      val fl = filename
      _: Unit => {
        val lines = Source.fromFile(fl).mkString.split('\n').toList
        new LocalSilo[String, List[String]](lines)
      }
    }).map { RDD(_) }
  }

  implicit def RDD2MapRDD[K, V, S <: Traversable[(K, V)]](rdd: RDD[(K, V), S]): MapRDD[K, V, S] = {
    new MapRDD[K, V, S](rdd.silos)
  }

  implicit def MapRDD2Semigroup[K, V : Semigroup, S <: Traversable[(K, V)]: Semigroup](rdd: MapRDD[K, V, S]): MapSemigroupRDD[K, V, S] =
    new MapSemigroupRDD(rdd.silos)

}

class RDD[T, S <: Traversable[T]] private[rdd] (val silos: Seq[SiloRef[T, S]]) {

  // Trick from https://stackoverflow.com/questions/3225675/can-i-pimp-my-library-with-an-analogue-of-traversablelike-map-that-has-nicely
  // Note
  type CanBuildTo[Elem, C] = CanBuildFrom[Nothing, Elem, C]

  def map[B, V <: Traversable[B]](f: Spore[T, B])(implicit cbt: CanBuildTo[B, V]): RDD[B, V] = {
    RDD(silos.map {
      s => s.apply[B, V](spore {
        val localFunc = f
        implicit val lCbt = cbt
        content => {
          content.map[B, V](localFunc)(collection.breakOut(lCbt))
        }
      })
    })
  }

  def filter(f: Spore[T, Boolean])(implicit cbf: CanBuildTo[T, S]): RDD[T, S] = {
    val resList = silos.map {
      s => s.apply[T, S](spore {
        val lf = f
        val lcbf = cbf
        content => {
          val builder = lcbf()
          content.foreach { elem => {
            if (lf(elem)) {
              builder += elem
            }
          } }
          builder.result()
        }
      })
    }
    RDD(resList)
  }

  def groupBy[K, IS <: Traversable[T], RS <: Traversable[(K, IS)]](f: Spore[T, K])(implicit cbf1: CanBuildTo[T, IS], cbf2: CanBuildTo[(K, IS), RS]): MapRDD[K, IS, RS] = {
    val resList = silos.map {
      s => s.apply[(K,  IS), RS](spore {
        val lf = f
        val lcbf1 = cbf1
        val lcbf2 = cbf2
        content => {
          val res0 = content.groupBy(lf)
          val b1 = lcbf1()
          val b2 = lcbf2()
          res0.foreach{case (k, vals) => {
            b1 ++= vals
            b2 += (k -> b1.result())
            b1.clear()
          }}
          b2.result()
        }
      })
    }
    new MapRDD[K, IS, RS](resList)
  }

  def flatMap[B, V <: Traversable[B]](f: Spore[T, Seq[B]])(implicit cbt1: CanBuildTo[B, V]): RDD[B, V] = {
    val resList = silos.map {
      s => s.apply[B, V](spore {
        val func = f
        implicit val lcbt1 = cbt1
        content => {
          content.flatMap[B, V](func)(collection.breakOut(lcbt1))
        }
      })
    }
    RDD(resList)
  }

  def union(other : RDD[T, S]): RDD[T, S] = RDD(silos ++ other.silos)

  def reduce(f: Spore2[T, T, T])(implicit ec: ExecutionContext): T = {
    val resList = silos.map {
      s => s.apply[T, List[T]](spore {
        val reducer = f
        content => {
          content.reduce(reducer) :: Nil
        }
      }).send()
    }
    val res = Future.sequence(resList)
    val res1 = Await.result(res, Duration.Inf).flatten
    res1.reduce(f)
  }

  def collect()(implicit ec: ExecutionContext): Seq[T] = {
    Await.result(Future.sequence(silos.map {
      s => s.send()
    }), Duration.Inf).flatten
  }

  def count()(implicit ec: ExecutionContext): Long = {
    val resList = silos.map {
      s => s.apply[Long, List[Long]](spore {
        content => content.size.longValue :: Nil
      }).send()
    }
    val res = Future.sequence(resList)
    val res1 = Await.result(res, Duration.Inf).flatten
    res1.reduce(_ + _)
  }
}
