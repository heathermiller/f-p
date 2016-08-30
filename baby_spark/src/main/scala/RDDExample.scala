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

  def lineageTest(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts.take(2)), Duration.Inf)
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt", hosts.take(2)), Duration.Inf)

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word))

    val res = contentWord.join(loremWord).silos
    printLineage(res(0))
    printLineage(res(1))
  }

  case class Tree[T](val data: T, left: Option[Tree[T]], right: Option[Tree[T]])

  def printLineage[T](sl: SiloRef[T]): Unit = {
    def print0(n: Node): Tree[Node] = {
      n match {
        case m : Materialized => Tree(m, None, None)
        case ap: Apply[t, s] => {
          val prev = Some(print0(ap.input))
          val fEnv = ap.fun.asInstanceOf[SporeWithEnv[t, s]]
          val right = None
          Tree(ap, prev, right)
        }
        case fm: FMapped[t, s] => {
          val prev = Some(print0(fm.input))
          val fEnv = fm.fun.asInstanceOf[SporeWithEnv[t, s]]
          val right = None
          Tree(fm, prev, right)
        }
      }
    }
    val res = print0(sl.asInstanceOf[ProxySiloRef[T]].node())

    def prettyPrint(tree: Tree[Node]): Unit = {
      print(s"Node:")
      tree.data match {
        case _: FMapped[_, _] => println(s"Flatmap@${tree.data.refId}")
        case _: Apply[_, _] => println(s"Apply@${tree.data.refId}")
        case _: Materialized => println(s"Materialied@${tree.data.refId}")
      }
      tree.left.map( e => {
        println("Going left")
        prettyPrint(e)
      })
      tree.right.map( e => {
        println("Going right")
        prettyPrint(e)
      })
      println("-----")
    }

    prettyPrint(res)
  }


  def joinExample(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts(0)), Duration.Inf)
    val lorem = Await.result(RDD.fromTextFile("data/lorem.txt", hosts(1)), Duration.Inf)
    val lorem2 = Await.result(RDD.fromTextFile("data/lorem.txt", hosts(2)), Duration.Inf)

    println("Got the content, join..")

    val partitioner = KeyPartitioner[Int, String](2)

    val contentWord = content.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word)).partition(partitioner).cache()

    val loremWord = lorem.flatMap(line => {
      line.split(' ').toList
    }).map(word => (word.length, word)).partition(partitioner).cache()

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

    val partitioner = new ProxyPartitioner((tup: (Int, String)) => tup._1, new HashPartitioner(2))

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

  def mapExample(system: SystemImpl, hosts: Seq[Host]): Unit = {
    implicit val sm = system
    val content = Await.result(RDD.fromTextFile("data/data.txt", hosts(0)), Duration.Inf)
    val lineLength = content.map(line => line.length)
    val twiceLength = lineLength.map(_ * 2).collect()
    val twiceLength2 = lineLength.map(_ * 2).collect()
    println(s"line length1: ${twiceLength} | line length 2: ${twiceLength2}")
    val bigLines = lineLength.filter(l => l > 30).count()
    println(s"There is ${bigLines} lines bigger than 30 characters")

    // val sizeLine = content.map[(Int, List[String]), TreeMap[Int, List[String]]](line => {
    //   val words = line.split(' ').toList
    //   (words.length, words)
    // }).collect()

    // println(s"Results.. ${sizeLine}")
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

    // val res1cached = Await.result(timeSiloCached.send(), 10.seconds)
    // val res1not = Await.result(timeSiloNotCached.send(), 10.seconds)
    // Thread.sleep(2000)
    // val res2cached = Await.result(timeSiloCached.send(), 10.seconds)
    // val res2not = Await.result(timeSiloNotCached.send(), 10.seconds)

    // println(s"Cache: Res 1 = $res1cached | Res2 = $res2cached")
    // println(s"Not cache: Res 1 = $res1not | Res2 = $res2not")
  }

  def main(args: Array[String]): Unit = {
    implicit val system = new SystemImpl
    val nActors = 5
    val started = system.start(nActors)
    val hosts = for (i <- 0 to nActors) yield { Host("127.0.0.1", 8090 + i)}

    Await.ready(started, 1.seconds)

    // println("Running examples")
    // externalDependencyExample(system)
    joinExample(system, hosts)
    // testPartition(system, hosts)
    // lineageTest(system, hosts)

    system.waitUntilAllClosed(30.seconds, 30.seconds)
  }
}
