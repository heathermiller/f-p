
import scala.collection.generic.CanBuildFrom

import scala.concurrent.duration._
import scala.concurrent.{ ExecutionContext, Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global

import scala.language.higherKinds

import scala.io.Source

import silt._
import silt.actors._

import scala.spores._
import SporePickler._

import scala.pickling._
import Defaults._

object Randomer {

  def simpleExample(system: SystemImpl, host: Host): Unit = {
    val silo = system.fromFun(host)(spore {
      _ => {
        val content = """Lorem ipsum dolor sit amet, consectetur adipiscing elit.
Phasellus condimentum aliquam felis nec ultrices. Nullam eu ante massa. Cras vehicula, orci sed varius malesuada, massa mauris pulvinar nibh, et eleifend eros odio id erat.
Duis vitae lacinia sapien. Ut sed placerat erat, quis feugiat lectus. Maecenas posuere felis eget nibh tincidunt, et hendrerit arcu vehicula. Nunc sodales justo lorem, vel interdum leo posuere et. Vestibulum sit amet felis non sem gravida condimentum at at lorem.
Quisque fringilla non lacus vel sodales. Integer vehicula et ex quis dictum. Morbi vitae eros viverra, finibus sapien in, faucibus odio. Duis condimentum pellentesque consectetur. Praesent in ante viverra nibh efficitur interdum quis vel purus. Etiam eget nisl eu sapien sodales sodales hendrerit in ex. """
        val lines = content.split('\n').toList
        new LocalSilo(lines)
      }
    })

    val fRes = silo.flatMap {
      s => {
        s.apply[List[String]](spore {
          content => content.flatMap(_.split(' '))
        }).send()
      }
    }

    val res = Await.result(fRes, 30.seconds)
    println(s"Result: ${res}")
  }

  def RDDExample(system: SystemImpl, host: Host): Unit = {

  }

  def main(args: Array[String]): Unit = {
    val system = new SystemImpl
    val started = system.start()
    Await.ready(started, 1.seconds)

    val host = Host("127.0.0.1", 8090)

    simpleExample(system, host)
    system.waitUntilAllClosed()
  }
}
