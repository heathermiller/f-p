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

import silt.{SiloRef, LocalSilo, Host}
import silt.actors._

case class Person(name: String, age: Int)
case class Vehicule(owner: Person, brand: String, year: Int)

object Main {

  /**
   * Create a silo from a filename.
   */
  def populateSilo(filename: String): LocalSilo[List[String]] = {
    val lines = Source.fromFile(filename).mkString.split('\n').toList
    new LocalSilo(lines)
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

  /**
   * Reproduce the flatMap example from the F-P paper.
   */
  def flatMapPaperEx(system: SystemImpl, host: Host): Future[List[(Person, Vehicule)]] = {
    // Generate 10_000 person and 50_000 vehicules.
    val pv = generateData(10000, 50000)

    // Initialize the two silo. `fromFun` returns a Future, hence the for-loop
    // below.
    val personsSilo = system.fromFun(host)(spore {
      val lPersons = pv._1
      _ => new LocalSilo[List[Person]](lPersons)
    })

    val vehiculesSilo = system.fromFun(host)(spore {
      val lVehicules = pv._2
      _ => new LocalSilo[List[Vehicule]](lVehicules)
    })

    for {
      pSilo <- personsSilo
      vSilo <- vehiculesSilo
      res <- {
        // Filter the persons to only get the adults ones.
        val adults = pSilo.apply[List[Person]](spore {
          ps => ps.filter(p => p.age >= 18)
        })

        // Calling `flatMap` on `adults` allows us to send its content using
        // spore to the other silo.
        val owners: Future[List[(Person, Vehicule)]] = adults.flatMap(spore {
          val lVehicules = vSilo
          ps => {
            lVehicules.apply[List[(Person, Vehicule)]](spore {
              // This is the line that indicate the data of `adults` is sent to
              // the `vehicules` silo (and node).
              val localPs = ps
              vs => {
                // Actual join
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

    val done = flatMapPaperEx(system, host)

    val res = Await.result(done, 60.seconds)
    println(s"Result: ${res.take(30)}")

    system.waitUntilAllClosed()
  }
}
