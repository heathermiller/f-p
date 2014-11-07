package silt
package demo

import scala.spores._

import scala.pickling._
import binary._

import silt.actors._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import scala.util.Random
import scala.math.Ordering

import scala.collection.immutable.TreeMap


case class Person(id: Int, age: Int, location: Int)

object PersonOrdering extends Ordering[Person] {
  def compare(x: Person, y: Person): Int = {
    x.id - y.id // negative means y.id is greater than x.id
  }
}

object Demo extends App {

  def populateSilo(): LocalSilo[Person, List[Person]] = {
    val persons: List[Person] = for (_ <- (1 to numPersons).toList) yield {
      val randomId  = Random.nextInt(10000000)
      val randomAge = Random.nextInt(100)
      val randomLoc = Random.nextInt(200) // country ID, say
      new Person(randomId, randomAge, randomLoc)
    }
    new LocalSilo(persons)
  }

  def doSearch(treeSilo: SiloRef[(Int, Person), TreeMap[Int, Person]], num: Int): SiloRef[Person, List[Person]] = {
    val range = 0 until num
    treeSilo.apply(spore {
      val localRange = range
      personsTree =>
        // test searching a few IDs
        val idToSearch = range.toList
        idToSearch.foreach { id =>
          val contains = personsTree.contains(id)
          if (id < 10 && contains)
            println(s"tree contains id $id: $contains")
        }
        val taken = personsTree.iterator.take(10)
        val res = taken.map(_._2)
        res.toList
    })
  }

  // called for each tree silo
  def localGroups(host: Host, silo: SiloRef[(Int, Person), TreeMap[Int, Person]]): List[SiloRef[Person, List[Person]]] = {
    // result type of groupBy: Map[Int, TreeMap[Int, Person]]
    val mapped: SiloRef[(Int, List[Person]), Map[Int, List[Person]]] =
      silo.apply(spore { x =>
        val grouped = x.groupBy(p => p._2.age / 25)
        grouped.mapValues { m =>
          val tmp = m.map(_._2)
          tmp.toList
        }
      })

    // on the same `host`, create 4 silos
    val ageGroupSilos = for (_ <- (1 to 4).toList) yield system.emptySilo[Person, List[Person]](host)
    // fill up silos according to age group
    ageGroupSilos.zipWithIndex.foreach {
      case (silo, i) =>
        mapped.pumpTo(silo)(spore {
          val localIndex = i
          (elem: (Int, List[Person]), emit: Emitter[Person]) =>
            if (elem._1 == localIndex) elem._2.foreach { person => emit.emit(person) }
        })
    }
    ageGroupSilos
  }

  implicit val theUnpickler = implicitly[Unpickler[Person]]

  // benchmark parameters
  val numPersons = 100000 //10000 //1000000
  val numSearches = 10000

  // create Silo system
  val system = new SystemImpl
  val started = system.start()
  Await.ready(started, 1.seconds)

  val hosts = for (port <- List(8090, 8091, 8092, 8093)) yield Host("127.0.0.1", port)


  // put into Silos
  val origSiloFuts = hosts.map { host => system.fromFun(host)(populateSilo) }
  val futOrigSilos = Future.sequence(origSiloFuts)

  val done = futOrigSilos.flatMap { origSilos =>
    // tree for each silo
    val treeSilos: List[SiloRef[(Int, Person), TreeMap[Int, Person]]] =
      for (silo <- origSilos) yield silo.apply[(Int, Person), TreeMap[Int, Person]] { persons =>
        var personsTree = TreeMap.empty[Int, Person]
        for (person <- persons) {
          personsTree = personsTree + (person.id -> person)
        }
        personsTree
      }

    // create a group for each silo
    val groups: List[List[SiloRef[Person, List[Person]]]] = for ((treeSilo, host) <- treeSilos.zip(hosts)) yield {
      localGroups(host, treeSilo)
    }

    val silos12 = for (i <- 0 until 4) yield
      system.emptySilo[Person, List[Person]](hosts(i % 2))

    groups(0).zip(groups(1)).zipWithIndex.map {
      case ((leftSilo, rightSilo), i) =>
        leftSilo.pumpTo(silos12(i))(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
        rightSilo.pumpTo(silos12(i))(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
    }

    val silos34 = for (i <- 0 until 4) yield
      system.emptySilo[Person, List[Person]](hosts((i % 2) + 2))

    groups(2).zip(groups(3)).zipWithIndex.map {
      case ((leftSilo, rightSilo), i) =>
        leftSilo.pumpTo(silos34(i))(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
        rightSilo.pumpTo(silos34(i))(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
    }

    // final merge
    val finalSilos = for (i <- 0 until 4) yield
      system.emptySilo[Person, List[Person]](hosts(i))

    silos12.zip(silos34).zipWithIndex.map {
      case ((leftSilo, rightSilo), i) =>
        leftSilo.pumpTo(finalSilos(i))(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
        rightSilo.pumpTo(finalSilos(i))(spore { (elem: Person, emit: Emitter[Person]) => emit.emit(elem) })
    }

    // check results
    val groupFuts = for (i <- 0 until 4) yield
      finalSilos(i).apply[Person, List[Person]](persons => persons.take(10)).send()
    Future.sequence(groupFuts)
  }

  // use apply to put it into a Silo with a search tree
  // closure passed to apply uses a "specialized" way of building the tree
/*
  val groups1 = fut1.flatMap { silo =>
    val treeSilo: SiloRef[(Int, Person), TreeMap[Int, Person]] = silo.apply { persons =>
      var personsTree: TreeMap[Int, Person] = TreeMap.empty[Int, Person]
      for (person <- persons) {
        personsTree = personsTree + (person.id -> person)
      }
      personsTree
    }

    val resSilo = doSearch(treeSilo, numSearches)

    groups
    resSilo.send()
  }
*/
  val res = Await.result(done, 20.seconds)
  println(s"res:\n${res.mkString("\n====\n")}")


  // shutdown Silo system
  system.waitUntilAllClosed()
}
