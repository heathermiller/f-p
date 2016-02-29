package baby_spark
package rdd

import scala.language.{higherKinds, implicitConversions}

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

// S == Type storing the (K, V) pairs
class MapRDD[K, V, S <: Traversable[(K, V)]](override val silos: Seq[SiloRef[(K, V), S]]) extends RDD[(K, V), S](silos) {
  def reduceByKey[RS <: Traversable[(K, V)]](f: Spore2[V, V, V])(implicit cbf1: CanBuildTo[(K, V), RS]): MapRDD[K, V, RS] = {
    val resList = silos.map {
      s => s.apply[(K, V), RS](spore {
        val func = f
        val lcbf = cbf1
        c => {
          val res0 = c.groupBy(_._1)
          val res1 = res0.map(e => (e._1, e._2.map(_._2)))
          val res2 = res1.map(e => (e._1, e._2.reduce(func)))(collection.breakOut(lcbf))
          res2
        }
      })
    }
    new MapRDD(resList)
  }

  // IS: Traversable type used to store the value for one key
  // RS: Traversable type used to store the mapping key/value
  def groupByKey[IS <: Traversable[V], RS <: Traversable[(K, IS)]]()(implicit cbf1: CanBuildTo[(K, IS), RS], cbf2: CanBuildTo[V, IS]): MapRDD[K, IS, RS] = {
    val resList = silos.map {
      s => s.apply[(K, IS), RS](spore {
        val lcbf = cbf1
        val lcbf2 = cbf2
        c => {
          val res0 = c.groupBy(_._1)
          val res1 = res0.map(e => (e._1, e._2.map(_._2)(collection.breakOut(lcbf2))))(collection.breakOut(lcbf))
          res1
        }
      })
    }
    new MapRDD[K, IS, RS](resList)
  }

  def join[IS <: Traversable[V], RS <: Traversable[(K, IS)]](other: MapRDD[K, V, S])(implicit cbf1: CanBuildTo[(K, IS), RS], cbf2: CanBuildTo[V, IS]): MapRDD[K, IS, RS] = {
    val rdd1 = groupByKey[IS, RS]()
    val rdd2 = other.groupByKey[IS, RS]()
    new MapRDD(rdd1.silos ++ rdd2.silos)
  }

  def join[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (V, W))]](other: MapRDD[K, W, S2])(implicit ec: ExecutionContext, cbf1: CanBuildTo[(K, (V, W)), FS], cbf2: CanBuildTo[(K, W), S2]): MapRDD[K, Tuple2[V, W], FS] = {
    flatMap[(K, Tuple2[V, W]), FS](spore {
      val rdd = other
      val lcbf1 = cbf1
      val lcbf2 = cbf2
      val lec = ec
      c1 => {
        val k1 = c1._1
        val v1 = c1._2
        val res0 = rdd.filter(spore {
          val lk1 = k1
          c2 => c2._1 == lk1
        })(lcbf2)
        val res1 = res0.map(spore {
          val lv1 = v1
          c2 => (c2._1, (lv1, c2._2))
        })(lcbf1)
        res1.collect()(lec)
      }
    })
  }
}

class MapSemigroupRDD[K, V : Semigroup, S <: Traversable[(K, V)] : Semigroup](override val silos: Seq[SiloRef[(K, V), S]]) extends MapRDD[K, V, S](silos) {

  def collectMap()(implicit ec: ExecutionContext): S = {
    Await.result(Future.sequence(silos.map {
      s => s.send()
    }), Duration.Inf).reduce((a, b) => a |+| b)
  }
}

object RDDExample {

  import silt.actors._
  import scala.concurrent._
  import scala.concurrent.duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit def TreeMapSemigroup[K, V : Semigroup](implicit ordering: scala.Ordering[K]): Semigroup[TreeMap[K, V]] = new Semigroup[TreeMap[K, V]] with std.MapInstances with std.MapFunctions {
    def zero = new TreeMap[K, V]()(ordering)
    // Repetition of scalaz.std.Map: method apppend defined in mapMonoid
    override def append(m1: TreeMap[K, V], m2: => TreeMap[K, V]): TreeMap[K, V] = {
      val m2Instance = m2

      val (from, to, semigroup) = {
        if (m1.size > m2Instance.size) (m2Instance, m1, Semigroup[V].append(_:V, _:V))
        else (m1, m2Instance, (Semigroup[V].append(_: V, _: V)).flip)
      }

      from.foldLeft(to) {
        case (to, (k, v)) => to.updated(k, to.get(k).map(semigroup(_, v)).getOrElse(v))
      }
    }
  }


  def main(args: Array[String]): Unit = {
    implicit val system = new SystemImpl
    val started = system.start()
    implicit val host = Host("127.0.0.1", 8090)

    Await.ready(started, 1.seconds)

    val content = Await.result(RDD.fromTextFile("data/data.txt"), Duration.Inf)
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt"), Duration.Inf)

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val res = contentWord.join[Set[String], TreeMap[Int, Set[String]]](loremWord).collectMap()

    println(s"Result... ${res}")

    // val lineLength = content.map(line => line.length)
    // val twiceLength = lineLength.map(_ * 2).collect()
    // val twiceLength2 = lineLength.map(_ * 2).collect()
    // println(s"line length1: ${twiceLength} | line length 2: ${twiceLength2}")
    // val bigLines = lineLength.filter(l => l > 30).count()
    // println(s"There is ${bigLines} lines bigger than 30 characters")

    // val sizeLine = content.map[(Int, List[String]), TreeMap[Int, List[String]]](line => {
    //   val words = line.split(' ').toList
    //   (words.length, words)
    // })


    system.waitUntilAllClosed()
  }
}
