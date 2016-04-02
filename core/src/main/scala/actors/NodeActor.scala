package silt
package actors

import scala.collection.mutable.HashMap
import scala.language.existentials

import akka.actor.{Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask

import scala.pickling._
import Defaults._
import binary._

import scala.spores._
import SporePickler._

import graph.Picklers._

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.mutable
import scala.collection.concurrent.TrieMap

import java.util.concurrent.atomic.AtomicInteger

import graph._


// emulates a node in the system
class NodeActor(system: SiloSystemInternal) extends Actor {

  implicit val timeout: Timeout = 300.seconds

  //TODO: do we need to use TrieMap here?
  // val builderOfEmitterId: mutable.Map[Int, (AbstractBuilder, Int, Int)] = new TrieMap[Int, (AbstractBuilder, Int, Int)]

  // maps SiloRef refIds to promises of local silo instances
  val promiseOf: mutable.Map[Int, Promise[LocalSilo[_]]] = new TrieMap[Int, Promise[LocalSilo[_]]]

  val numPickled = new AtomicInteger(0)

  val cacheMap: mutable.Map[Node, LocalSilo[_]] = new HashMap[Node, LocalSilo[_]]

  def getOrElseInitPromise(id: Int): Promise[LocalSilo[_]] = promiseOf.get(id) match {
    case None =>
      println("no promise found")
      val newPromise = Promise[LocalSilo[_]]()
      promiseOf += (id -> newPromise)
      newPromise
    case Some(promise) =>
      println("found promise")
      promise
  }

  def resetPromise(id: Int): Unit = promiseOf -= id

  def respondSilo(promise: Promise[LocalSilo[_]], sender: ActorRef, silo: LocalSilo[_], refId: Int): Unit = {
    promise.success(silo)
    println(s"responding to ${sender.path.name}")
    sender ! ForceResponse(silo.value)
    resetPromise(refId)
  }

  def receive = {
    case Terminate() =>
      println(s"number objects pickled: ${numPickled.get}")
      context.stop(self)

    case theMsg @ InitSilo(fqcn, refId) =>
      println(s"SERVER: creating silo using class $fqcn...")

      val promise = getOrElseInitPromise(refId)

      // IDEA: same pattern for spores, again!
      val s = sender
      Future {
        try {
          val clazz = Class.forName(fqcn)
          println(s"SERVER: looked up $clazz")
          val inst = clazz.newInstance()
          println(s"SERVER: created instance $inst")
          inst match {
            case factory: SiloFactory[t] =>
              val newSilo = factory.data // COMPUTE-INTENSIVE
              promise.success(newSilo)

              println(s"SERVER: created $newSilo. responding...")
              val replyMsg = OKCreated(refId)
              replyMsg.id = theMsg.id
              s ! replyMsg
            case _ => println(s"Can't do anything, type of inst is ${inst.getClass}")
          }
        } catch {
          case e: Exception => {
            println("EXCEPTION EXCEPTION EXCEPTION")
            e.printStackTrace()
            s ! ForceError(e)
          }
        }
      }

    case theMsg @ InitSiloFun(fun, refId) =>
      println(s"SERVER: creating silo using class $fun...")

      val promise = getOrElseInitPromise(refId)

      // IDEA: same pattern for spores, again!
      val s = sender
      Future {
        try {
          val newSilo = fun(())
          promise.success(newSilo)

          println(s"SERVER: created $newSilo. responding...")
          val replyMsg = OKCreated(refId)
          replyMsg.id = theMsg.id
          s ! replyMsg
        } catch {
          case e: Exception => {
            println("EXCEPTION EXCEPTION EXCEPTION")
            e.printStackTrace()
            s ! ForceError(e)
          }
        }
      }

    case msg @ ForceMessage(idVal) =>
      println(s"SERVER: forcing SiloRef '$idVal'...")
      println(s"force message promises: ${promiseOf}")

      // check if promise exists, if not insert empty promise
      // (right now, this is atomic due to atomic turn property of actors)
      val promise = getOrElseInitPromise(idVal)

      // IDEA: illustrate use of spores in paper using this code as example for distributed systems code!
      val s = sender
      promise.future.foreach { silo =>
        println(s"Silo $idVal available. completing force...")
        val replyMsg = ForceResponse(silo.value)
        replyMsg.id = msg.id
        s ! replyMsg
      }

    case msg @ ForceError(e) =>
      println("Received an error from one of the node:")
      e.printStackTrace()

    case msg @ Graph(n, cache) =>
      println(s"node actor: received graph with node $n")
      println(s"graph is: ${msg}")

      n match {
        // expect a ForceResponse(value)
        case app: Apply[t, s] =>
          val fun = app.fun
          val promise = getOrElseInitPromise(app.refId)
          val localSender = sender
          if (cacheMap.contains(n)) {
            val res = cacheMap(n)
            respondSilo(promise, localSender, res, app.refId)
          }
          else {
            (self ? Graph(app.input, false)).map { case ForceResponse(value) =>
              println(s"yay: input graph is materialized")
              try {
                val res = fun(value.asInstanceOf[t])
                val newSilo = new LocalSilo[s](res)
                if (cache) {
                  cacheMap += (n -> newSilo)
                } else {
                  respondSilo(promise, localSender, newSilo, app.refId)
                }
              } catch {
                case e: Exception => {
                  println("EXCEPTION EXCEPTION EXCEPTION")
                  e.printStackTrace()
                  localSender ! ForceError(e)
                }
              }
            }
          }

        case fm: FMapped[t, s] =>
          val fun = fm.fun
          val promise = getOrElseInitPromise(fm.refId)
          val localSender = sender
          if (cacheMap.contains(n)) {
            val res = cacheMap(n)
            respondSilo(promise, localSender, res, fm.refId)
          }
          (self ? Graph(fm.input, false)).map { case ForceResponse(value) =>
            try {
              val resSilo = fun(value.asInstanceOf[t])
              val res = resSilo.send()
              res.map { case data =>
                val newSilo = new LocalSilo[s](data)
                if (cache) {
                  cacheMap += (n -> newSilo)
                } else {
                  respondSilo(promise, localSender, newSilo, fm.refId)
                }
              }
            } catch {
              case e: Exception => {
                println("EXCEPTION EXCEPTION EXCEPTION")
                e.printStackTrace()
                localSender ! ForceError(e)
              }
            }
          }

        case m: Materialized =>
          val s = sender
          promiseOf(m.refId).future.foreach { (silo: LocalSilo[_]) =>
            s ! ForceResponse(silo.value)
          }

      }

  }
}
