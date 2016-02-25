package baby_spark

import scala.io.Source

import scala.util.Random

import scala.spores._
import SporePickler._

import scala.pickling._
import Defaults._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import silt.{SiloRef, LocalSilo, Host, Emitter}
import silt.actors._

case class Person(name: String, age: Int)
case class Vehicule(owner: Person, brand: String, year: Int)

object Main {

  def populateSilo(filename: String): LocalSilo[String, List[String]] = {
    val lines = Source.fromFile(filename).mkString.split('\n').toList
    new LocalSilo(lines)
  }

  def flatMapEx(system: SystemImpl, host: Host): Future[List[Int]] = {
    val silo = system.fromFun(host)(spore {
      _ => populateSilo ("data/data.txt")
    })

    def filterData(l: List[List[String]]): List[Int] = {
      l.flatten.filter(s => !(Set(",", ".", "!").contains(s))).length :: Nil
    }

    val done = silo.flatMap {
      s => {
        s.flatMap(spore {
          val localSilo = s
          val f = filterData _
          _: List[String] => {
            val splitted = localSilo.apply[List[String], List[List[String]]](spore {
              l: List[String] => l.map(_.split(' ').toList)
            })

            splitted.apply[Int, List[Int]](spore {
              val f2 = f
              l: List[List[String]] => f2(l)
              // l: List[List[String]] => {
              //   val res1 = l.flatten
              //   val filterSet = Set(",", ".", "!")
              //   val res2 = res1.filter(s => !filterSet.contains(s))
              //   val res3 = res2.length
              //   List(res3)
              // }
            })
          }
        }).send()
      }
    }

    return done
  }

  def generateData(nPerson: Int, nVehicule: Int): (List[Person], List[Vehicule]) = {
    def randomString(length: Int) = Random.alphanumeric.take(length).mkString

    val persons = for (_ <- 1 to nPerson) yield {
      Person(randomString(3 + Random.nextInt(7)), 5 + Random.nextInt(85))
    }

    val vehicules = for (_ <- 1 to nVehicule) yield {
      Vehicule(persons(Random.nextInt(persons.size)), randomString(3 + Random.nextInt(7)), 1960 + Random.nextInt(55))
    }

    (persons.toList, vehicules.toList)
  }

  // Reproduce the example from the F-P paper
  def flatMapPaperEx(system: SystemImpl, host: Host): Future[List[(Person, Vehicule)]] = {
    val pv = generateData(10000, 50000)

    val personsSilo = system.fromFun(host)(spore {
      val lPersons = pv._1
      _ => new LocalSilo[Person, List[Person]](lPersons)
    })

    val vehiculesSilo = system.fromFun(host)(spore {
      val lVehicules = pv._2
      _ => new LocalSilo[Vehicule, List[Vehicule]](lVehicules)
    })

    for {
      pSilo <- personsSilo
      vSilo <- vehiculesSilo
      res <- {
        val adults = pSilo.apply[Person, List[Person]](spore {
          ps => ps.filter(p => p.age >= 18)
        })

        val owners: Future[List[(Person, Vehicule)]] = adults.flatMap(spore {
          val lVehicules = vSilo
          ps => {
            lVehicules.apply[(Person, Vehicule), List[(Person, Vehicule)]](spore {
              val localPs = ps
              vs => {
                localPs.flatMap(p => {
                  vs.flatMap { v => if (v.owner.name == p.name) List((p, v)) else Nil }
                })
              }
            })
          }
        }).send()

        owners
      }
    } yield { res }
  }

  def main(args: Array[String]) = {
    val system = new SystemImpl
    val started = system.start()
    Await.ready(started, 1.seconds)

    val host = Host("127.0.0.1", 8090)

    // val done = flatMapEx(system, host)
    val done = flatMapPaperEx(system, host)

    val res = Await.result(done, 60.seconds)
    println(s"Result: ${res.take(30)}")


    system.waitUntilAllClosed()
  }
}
