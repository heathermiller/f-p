package netty

import scala.pickling._
import Defaults._
import shareNothing._

import scala.spores._
import SporePickler._

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.mutable.ListBuffer

import silt.{SiloRef, Host, LocalSilo, Emitter}
import silt.netty.{SendUtils, SystemImpl, Server, TestSiloFactory, Person}


object WordCountMultiJvmNode1 {
  runtime.GlobalRegistry.picklerMap += ("silt.graph.CommandEnvelope" -> { x => silt.graph.Picklers.CommandEnvelopePU })
  runtime.GlobalRegistry.unpicklerMap += ("silt.graph.CommandEnvelope" -> silt.graph.Picklers.CommandEnvelopePU)

  def main(args: Array[String]): Unit =
    new Server(8091, new SystemImpl).run()
}

object WordCountMultiJvmNode2 {
  runtime.GlobalRegistry.picklerMap += ("silt.graph.CommandEnvelope" -> { x => silt.graph.Picklers.CommandEnvelopePU })
  runtime.GlobalRegistry.unpicklerMap += ("silt.graph.CommandEnvelope" -> silt.graph.Picklers.CommandEnvelopePU)

  def main(args: Array[String]): Unit =
    new Server(8092, new SystemImpl).run()
}

object WordCountMultiJvmNode3 {
  runtime.GlobalRegistry.picklerMap += ("silt.graph.CommandEnvelope" -> { x => silt.graph.Picklers.CommandEnvelopePU })
  runtime.GlobalRegistry.unpicklerMap += ("silt.graph.CommandEnvelope" -> silt.graph.Picklers.CommandEnvelopePU)

  def randomWord(random: Random): String = {
    //TODO: read in list of words from big file
    val words = Array("is", "computer", "apple", "orange", "chair", "work", "fun", "program", "paper")
    val index = random.nextInt(words.length)
    words(index)
  }

  def populateSilo(numLines: Int, random: Random): LocalSilo[String, List[String]] = {
    // each string is a concatenation of 10 random words, separated by space
    val buffer = ListBuffer[String]()
    val lines = for (i <- 0 until numLines) yield {
      val tenWords = for (_ <- 1 to 10) yield randomWord(random)
      buffer += tenWords.mkString(" ")
    }
    new LocalSilo(buffer.toList)
  }

  implicit val sp1 = implicitly[Pickler[Spore[List[String], List[(String, Int)]]]]
  implicit val sp2 = implicitly[Pickler[Spore[List[(String, Int)], List[(String, Int)]]]]
  implicit val sup1 = implicitly[Unpickler[Spore[List[String], List[(String, Int)]]]]
  implicit val sup2 = implicitly[Unpickler[Spore[List[(String, Int)], List[(String, Int)]]]]

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000) // FIXME

    val system = new SystemImpl
    val host1 = Host("127.0.0.1", 8091)
    val host2 = Host("127.0.0.1", 8092)

    val silo1Fut = system.fromFun(host1)(() => populateSilo(10, new Random(100)))
    val silo2Fut = system.fromFun(host2)(() => populateSilo(10, new Random(200)))

    val reducedPairs: SiloRef[String, List[String]] => SiloRef[(String, Int), List[(String, Int)]] =
      { (silo: SiloRef[String, List[String]]) =>
        val simplePairs = silo.apply[(String, Int), List[(String, Int)]](spore { (lines: List[String]) =>
          lines.flatMap { line =>
            val words = line.split(' ')
            for (word <- words) yield (word, 1)
          }
        })

        // insert into map, reduce by key
        simplePairs.apply[(String, Int), List[(String, Int)]](spore { (pairs: List[(String, Int)]) =>
          val m = pairs.groupBy(pair => pair._1)
          val resMap = m.mapValues(l => l.size)
          resMap.toList
        })
      }

    val target = system.emptySilo[(String, Int), List[(String, Int)]](host1)

    val res1Fut = silo1Fut.flatMap { silo =>
      val tmp = reducedPairs(silo)
      val s = spore { (elem: (String, Int), emit: Emitter[(String, Int)]) => emit.emit(elem) }
      tmp.pumpTo(target)(s)
      tmp.send()
    }
    val res1 = Await.result(res1Fut, 5.seconds)
    println(s"res1: $res1")

    assert(res1.contains("program" -> 18))

    val res2Fut = silo2Fut.flatMap { silo =>
      val tmp = reducedPairs(silo)
      val s = spore { (elem: (String, Int), emit: Emitter[(String, Int)]) => emit.emit(elem) }
      tmp.pumpTo(target)(s)
      tmp.send()
    }
    val res2 = Await.result(res2Fut, 5.seconds)
    println(s"res2: $res2")

    val finalSilo = target.apply[(String, Int), List[(String, Int)]](spore { (pairs: List[(String, Int)]) =>
      val m = pairs.groupBy(pair => pair._1)
      val resMap = m.mapValues { l =>
        val tmp = l.map(elem => elem._2)
        tmp.reduce(_ + _)
      }
      resMap.toList
    })
    val finalResFut = finalSilo.send()
    val finalRes = Await.result(finalResFut, 5.seconds)
    println(s"finalRes: $finalRes")

    assert(finalRes.contains("program" -> 29))

    system.waitUntilAllClosed()
  }
}
