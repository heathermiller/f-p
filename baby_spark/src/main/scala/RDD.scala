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
    return new MapRDD[K, V, S](rdd.silos)
  }

  implicit def MapRDD2RDD[K, V, S <: Traversable[(K, V)]](rdd: MapRDD[K, V, S]): RDD[(K, V), S] = {
    return new RDD[(K, V), S](rdd.silos)
  }

  // implicit def TupleRDD2PairRDD[K, V, S <: Traversable[(K, V)]](rdds: Tuple2[MapRDD[K, V, S], MapRDD[K, V, S]]): PairRDD[K, V, S] = {
  //   return new PairRDD[K, V, S](rdds._1.silos, rdds._2.silos)
  // }
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
  def reduceByKey(f: Spore2[V, V, V]): MapRDD[K, V, S] = ???

  def groupByKey(): MapRDD[K, V, S] = ???

  // TJoin: Traversable to use to store the Tuple(V, V2)
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
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt"), Duration.Inf)

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val res = contentWord.join(loremWord).collect()

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
