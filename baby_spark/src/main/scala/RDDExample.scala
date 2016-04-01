package baby_spark


import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.immutable.{TreeMap, Map}
import scala.io.Source

import scalaz._
import Scalaz._

import scala.spores._
import scala.pickling._
import Defaults._
import SporePickler._

import baby_spark.rdd._

import com.typesafe.config.ConfigFactory

import silt._
import silt.actors._

object RDDExample {

  implicit def TreeMapSemigroup[K, V : Semigroup]
    (implicit ordering: scala.Ordering[K]): Semigroup[TreeMap[K, V]] =
    new Semigroup[TreeMap[K, V]] with std.MapInstances with std.MapFunctions {

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

    val res = res1.join[Set, Map](res2).collectMap()

    println(s"Result.. ${res}")
  }

  def joinExample(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts(0)), Duration.Inf)
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt", hosts(1)), Duration.Inf)
    val lorem2 = Await.result(RDD.fromTextFile("data/lorem.txt", hosts(2)), Duration.Inf)

    println("Got the content, join..")

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    // val loremWord2 = lorem2.flatMap(line => {
    //   line.split(' ').toList
    // }).map(word => (word.length, word)).groupByKey[Set, TreeMap]()

    val res = contentWord.join[Set, Map](loremWord).collectMap()
    // val res = contentWord.join[Set, TreeMap](loremWord).union(loremWord2).collectMap()

    println(s"Result... ${res}")
  }

  def mapExample(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts(0)), Duration.Inf)
    val lineLength = content.map(line => line.length)
    val twiceLength = lineLength.map(_ * 2).collect()
    val twiceLength2 = lineLength.map(_ * 2).collect()
    println(s"line length1: ${twiceLength} | line length 2: ${twiceLength2}")
    val bigLines = lineLength.filter(l => l > 30).count()
    println(s"There is ${bigLines} lines bigger than 30 characters")

    val sizeLine = content.map[(Int, List[String]), TreeMap[Int, List[String]]](line => {
      val words = line.split(' ').toList
      (words.length, words)
    }).collect()

    println(s"Results.. ${sizeLine}")
  }

  def externalDependencyExample(system: SystemImpl): Unit = {
    val host = Host("127.0.0.1", 8090)

    trait WithName {
      def name: String
    }
    case class Person(name: String, age: Int) extends WithName

    val persons = for(i <- 0 until 100) yield Person(s"foo: {i}", i)

    val silo = Await.result(system.fromFun(host)(spore {
      val lPersons = persons
      _: Unit => {
        new LocalSilo[List[Person]](lPersons.toList)
      }
    }), 30.seconds)

    println("Created externalDep local silo")

    def caseClass(): Unit = {

      val resSilo = silo.apply[List[String]](spore {
        ps => {
          ps.map(_.name)
        }
      }).send()

      val res = Await.result(resSilo, 10.seconds)
      println(s"Result of caseClass: ${res.size}")
    }

    def genericClass(): Unit = {
      class Gen[T <: WithName](val content: T) {
        def toName(): String = content.name
      }

      val sp = spore[List[Person], List[Gen[Person]]] {
        ps => {
          ps.map(new Gen(_))
        }
      }

      val resSilo = silo.apply[List[Gen[Person]]](sp).send()

      val res = Await.result(resSilo, 10.seconds)
      println(s"Result of genClass: ${res.size}")
    }

    println("Running case class")
    caseClass()
    println("Running genClass")
    genericClass()
  }

  def testCache(system: SystemImpl, hosts: Seq[Host]): Unit = {
    val fTime = java.lang.System.currentTimeMillis _

    val silo = Await.result(system.fromFun(hosts(0))(spore {
      _: Unit => {
        new LocalSilo[List[Int]](List((1)))
      }
    }), 10.seconds)

    val timeSiloCached = silo.apply[List[Long]](spore {
      val f = fTime
      c => {
        val time = f()
        println(s"Time is: $time")
        c.map(x => time)
      }
    }).cache()

    val timeSiloNotCached = silo.apply[List[Long]](spore {
      val f = fTime
      c => {
        val time = f()
        println(s"Time is: $time")
        c.map(x => time)
      }
    })

    val res1cached = Await.result(timeSiloCached.send(), 10.seconds)
    val res1not = Await.result(timeSiloNotCached.send(), 10.seconds)
    Thread.sleep(2000)
    val res2cached = Await.result(timeSiloCached.send(), 10.seconds)
    val res2not = Await.result(timeSiloNotCached.send(), 10.seconds)

    println(s"Cache: Res 1 = $res1cached | Res2 = $res2cached")
    println(s"Not cache: Res 1 = $res1not | Res2 = $res2not")
  }

  def main(args: Array[String]): Unit = {
    implicit val system = new SystemImpl
    val nActors = 5
    val started = system.start(nActors)
    val hosts = for (i <- 0 to nActors) yield { Host("127.0.0.1", 8090 + i)}

    Await.ready(started, 1.seconds)

    // println("Running examples")
    // externalDependencyExample(system)
    multiSiloRDD(system, hosts, 2)

    system.waitUntilAllClosed(30.seconds, 30.seconds)
  }
}
