package baby_spark


import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable.{TreeMap, Map}
import scala.io.Source

import scala.spores._
import scala.pickling._
import Defaults._
import SporePickler._

import baby_spark.rdd._

import com.typesafe.config.ConfigFactory

import silt._
import silt.actors._
import silt.graph.{ Apply, ApplySiloRef, FMapped, FMappedSiloRef, Materialized, Node, ProxySiloRef }


object RDDExample {

  def multiSiloRDD(system: SystemImpl, hosts: Seq[Host], n: Int): Unit = {
    assert(hosts.length >= n)

    def initSilo(): Seq[SiloRef[List[String]]] = {
      val silosFutures = hosts.take(n).zipWithIndex.map{ case (h, i) => {
        system.fromFun(h)(spore {
          val index = i + 1
          _: Unit => {
            val lines = Source.fromFile(s"data/lorem${index}.txt").mkString.split('\n').toList
            new LocalSilo[List[String]](lines)
          }
        })
      }}
      Await.result(Future.sequence(silosFutures), 10.seconds)
    }

    val silos = initSilo().splitAt(n / 2)
    val rdd1 = RDD[String, List[String]](silos._1)
    val rdd2 = RDD[String, List[String]](silos._2)

    def splitWords(rdd: RDD[String, List[String]]): RDD[(Int, String), List[(Int, String)]] =
    {
      rdd.flatMap(line => {
        line.split(' ').toList
      }).map(word => (word.length, word))
    }

    val res1 = splitWords(rdd1)
    val res2 = splitWords(rdd2)

    // val res = res1.join(res2).collectMap()

    // println(s"Result.. ${res}")
  }


  /**
   * Split two textfiles into (word.length, word) tuple.
   */
  def joinExample(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts(0)), Duration.Inf)
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt", hosts(1)), Duration.Inf)

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word)).cache()

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word)).cache()

    val res = contentWord
      .join(loremWord)
      .combine[String, Set, Map]()
      .collect()

    println(s"Result: ${res}")
  }

  def testPartition(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts.take(2)), Duration.Inf)
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt", hosts.take(2)), Duration.Inf)

    // val partitioner = new ProxyPartitioner((tup: (Int, String)) => tup._1, new HashPartitioner(2))
    val partitioner = new KeyPartitioner[Int, String](2)

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word)).partition(partitioner).cache()

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word)).partition(partitioner).cache()

    val contentRes = Future.sequence(contentWord.silos.map { x => x.send() })
    val loremRes = Future.sequence(loremWord.silos.map { x => x.send() })

    for {
      res1 <- contentRes
      res2 <- loremRes
    } yield {
      def f(silos: Seq[Seq[(Int, String)]]): Seq[(Int, Seq[Int])] = {
        silos.zipWithIndex.map { case (data, partNum) => {
          val keys = data.map(_._1).distinct
          (partNum -> keys)
        }}
      }
      val keys1 = f(res1)
      val keys2 = f(res2)
      keys1.map { case (i, keys) => {
        keys2.filter(_._1 != i).map { e =>
          assert((e._2.toSet & keys.toSet) == Set())
        }
      }}
    }
  }

  def testCache(system: SystemImpl, hosts: Seq[Host]): Unit = {
    val fTime = java.lang.System.currentTimeMillis _

    val silo = Await.result(system.fromFun(hosts(0))(spore {
      _: Unit => {
        new LocalSilo[Unit](())
      }
    }), 10.seconds)

    val timeSiloCached = silo.apply[Long](spore {
      val f = fTime
      c => {
        val time = f()
        time
      }
    }).cache()

    val timeSiloNotCached = silo.apply[Long](spore {
      val f = fTime
      c => {
        val time = f()
        time
      }
    })

    val (res1cached, res1not) = Await.result(for {
      tsCached <- timeSiloCached
      res1cached <- tsCached.send()
      res1not <- timeSiloNotCached.send()
    } yield { (res1cached, res1not) }, 10.seconds)

    Thread.sleep(2000)

    val (res2cached, res2not) = Await.result(for {
      tsCached <- timeSiloCached
      res2cached <- tsCached.send()
      res2not <- timeSiloNotCached.send()
    } yield { (res2cached, res2not) }, 10.seconds)

    println(s"Cache: Res 1 = $res1cached | Res2 = $res2cached")
    println(s"Not cache: Res 1 = $res1not | Res2 = $res2not")
    assert(res1cached == res2cached)
    assert(res1not != res2not)
  }

  def logisticRegression(system: SystemImpl, hosts: Seq[Host], filename: String): Unit = {
    implicit val sm = system

    case class Point(components: Seq[Float]) {
      def dot(p: Point): Float = {
        components.zip(p.components).map { case (p1, p2) => p1 * p2 }.sum
      }
      def *(v: Float): Point = Point(components.map(_ * v))
      def +(p: Point): Point = Point(components.zip(p.components).map { case (p1, p2) => p1 + p2 })
      def -(p: Point): Point = Point(components.zip(p.components).map { case (p1, p2) => p1 - p2 })
    }

    case class Observation(x: Point, output: Boolean) {
      def y: Int = if (output) 1 else 0
    }


    def parseLine(line: String): Observation = {
      val splitted = line.split(',')
      // add the 1 vector corresponding to beta0
      Observation(Point(splitted.slice(0, 10).map(_.toFloat)), splitted(10).toInt == 1)
    }

    val points = Await.result(RDD.fromTextFile(filename, hosts), 10.seconds)
      .map(spore {
        val f = parseLine _
        l => f(l)
      }).cache()

    val ITERATIONS = 50
    var w = Point(Seq.fill(10)(0))

    def grad(w: Point, p: Observation): Point = {
        p.x * (1/(1 + Math.exp(-p.y * (w dot p.x))).toFloat - 1) * p.y
    }

    for (i <- 1 to ITERATIONS) {
      val gradient = points.map(spore {
        val fGrad = grad _
        val lw = w
        p => fGrad(lw, p)
      }).reduce(spore {
        (p1, p2) => p1 + p2
      })
      w -= gradient
    }

    println(s"Resulting gradient is: $w")
  }

  def main(args: Array[String]): Unit = {
    implicit val system = new SystemImpl
    val nActors = 5
    val started = system.start(nActors)
    val hosts = for (i <- 0 to nActors) yield { Host("127.0.0.1", 8090 + i)}

    Await.ready(started, 1.seconds)

    println("Running examples")
    // testCache(system, hosts)
    // mapExample(system, hosts)
    logisticRegression(system, hosts, "data/ds1.10.csv")
    // joinExample(system, hosts)
    // testPartition(system, hosts)

    system.waitUntilAllClosed(30.seconds, 30.seconds)
  }
}
