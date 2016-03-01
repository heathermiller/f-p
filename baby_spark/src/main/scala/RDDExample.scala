package baby_spark

import silt.actors._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable.TreeMap

import scalaz._
import Scalaz._

import baby_spark.rdd._

import silt._

object RDDExample {


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

    val res = contentWord.join[Set[String], Map[Int, Set[String]]](loremWord).collectMap()

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
