package samples
package getstarted

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.pickling.Defaults._
import scala.pickling.shareNothing._

import scala.spores._
import SporePickler._

import silt.{SiloSystem, Host, LocalSilo}

object Client {

  private val summary = """
In this talk, I'll present some of our ongoing work on a new programming model
for asynchronous and distributed programming. For now, we call it "function-passing"
or "function-passing style", and it can be thought of as an inversion
of the actor model - keep your data stationary, send and apply your
functionality (functions/spores) to that stationary data, and get typed
communication all for free, all in a friendly collections/futures-like
package!
"""

  private lazy val words = summary.replace('\n', ' ').split(" ")

  def randomWord(random: scala.util.Random): String = {
    val index = random.nextInt(words.length)
    words(index)
  }

  def populateSilo(numLines: Int, random: scala.util.Random): LocalSilo[List[String]] = {
    // each string is a concatenation of 10 random words, separated by space
    val buffer = collection.mutable.ListBuffer[String]()
    val lines = for (i <- 0 until numLines) yield {
      val tenWords = for (_ <- 1 to 10) yield randomWord(random)
      buffer += tenWords.mkString(" ")
    }
    new LocalSilo(buffer.toList)
  }

  def main(args: Array[String]): Unit = {
    val system = SiloSystem()
    val host = Host("127.0.0.1", 8090)

    val siloFut = system.fromFun(host)(spore { (x: Unit) =>
      populateSilo(10, new scala.util.Random(100))
    })
    val done = siloFut.flatMap(_.send())

    val res = Await.result(done, 15.seconds)
    println("RESULT:")
    println(s"size of list: ${res.size}")
    res.foreach(println)

    system.waitUntilAllClosed()
  }

}
