package silt
package actors

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
  val builderOfEmitterId: mutable.Map[Int, (AbstractBuilder, Int, Int)] = new TrieMap[Int, (AbstractBuilder, Int, Int)]

  // maps SiloRef refIds to promises of local silo instances
  val promiseOf: mutable.Map[Int, Promise[LocalSilo[_, _]]] = new TrieMap[Int, Promise[LocalSilo[_, _]]]

  val numPickled = new AtomicInteger(0)

  def getOrElseInitPromise(id: Int): Promise[LocalSilo[_, _]] = promiseOf.get(id) match {
    case None =>
      println("no promise found")
      val newPromise = Promise[LocalSilo[_, _]]()
      promiseOf += (id -> newPromise)
      newPromise
    case Some(promise) =>
      println("found promise")
      promise
  }

  class RemoteEmitter[T](destNodeActor: ActorRef, emitterId: Int, destRefId: Int) extends Emitter[T] {
    def emit(v: T)(implicit pickler: Pickler[T], unpickler: Unpickler[T]): Unit = {
      // println(s"using pickler of class type ${pickler.getClass.getName} to pickle $v")

      // val p = v.pickle

      try {
        // 1. pickle value
/*
        val builder = pickleFormat.createBuilder()
        builder.hintTag(pickler.tag)
        pickler.pickle(v, builder)
        val p = builder.result()
*/
        val p = (v: Any).pickle
        numPickled.incrementAndGet()

        // 2. create SelfDescribing instance
        val sd = SelfDescribing(unpickler.getClass.getName, p.value)

        // 3. pickle SelfDescribing instance
        val sdp = sd.pickle
        val ba = sdp.value
        destNodeActor ! Emit(emitterId, destRefId, ba)
      } catch {
        case t: Throwable =>
          println(s"caught $t")
          t.printStackTrace()
      }
    }
    def done(): Unit = {
      destNodeActor ! Done(emitterId, destRefId)
    }
  }

  def mkEmitter[T, R](emitterId: Int, destHost: Host, destRefId: Int, builder: AbstractBuilder { type Elem = T; type Coll = R }): Future[Emitter[T]] = {
    val destNodeActor = Config.m(destHost)

    val createSiloMsg = CreatSilo(emitterId, destRefId, builder)

    (destNodeActor ? createSiloMsg).map { x =>
      new RemoteEmitter[T](destNodeActor, emitterId, destRefId)
    }
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
        val clazz = Class.forName(fqcn)
        println(s"SERVER: looked up $clazz")
        val inst = clazz.newInstance()
        println(s"SERVER: created instance $inst")
        inst match {
          case factory: SiloFactory[u, t] =>
            val newSilo = factory.data // COMPUTE-INTENSIVE
            promise.success(newSilo)

            println(s"SERVER: created $newSilo. responding...")
            val replyMsg = OKCreated(refId)
            replyMsg.id = theMsg.id
            s ! replyMsg

          case _ => /* do nothing */
        }
      }

    case theMsg @ InitSiloFun(fun, refId) =>
      println(s"SERVER: creating silo using class $fun...")

      val promise = getOrElseInitPromise(refId)

      // IDEA: same pattern for spores, again!
      val s = sender
      Future {
        val newSilo = fun(())
        promise.success(newSilo)

        println(s"SERVER: created $newSilo. responding...")
        val replyMsg = OKCreated(refId)
        replyMsg.id = theMsg.id
        s ! replyMsg
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

    case msg @ Graph(n) =>
      // println(s"node actor: received graph with node $n")

      n match {
        // expect a ForceResponse(value)
        case app: Apply[u, t, v, s] =>
          val fun = app.fun
          val promise = getOrElseInitPromise(app.refId)
          val localSender = sender
          (self ? Graph(app.input)).map { case ForceResponse(value) =>
            println(s"yay: input graph is materialized")
            val res = fun(value.asInstanceOf[t])
            val newSilo = new LocalSilo[v, s](res)
            promise.success(newSilo)
            println(s"responding to ${localSender.path.name}")
            localSender ! ForceResponse(res)
          }

        case fm: FMapped[u, t, v, s] =>
          val fun = fm.fun
          val promise = getOrElseInitPromise(fm.refId)
          val localSender = sender
          (self ? Graph(fm.input)).map { case ForceResponse(value) =>
            val resSilo = fun(value.asInstanceOf[t])
            val res = resSilo.send()
            res.map { case data =>
              val newSilo = new LocalSilo[v, s](data)
              promise.success(newSilo)
              localSender ! ForceResponse(data)
            }
          }

        case m: Materialized =>
          val s = sender
          promiseOf(m.refId).future.foreach { (silo: LocalSilo[_, _]) =>
            s ! ForceResponse(silo.value)
          }

        case m: MultiInput[r] =>
          val inputs    = m.inputs
          val refId     = m.refId
          val emitterId = m.emitterId

          val s = sender
          val promise = getOrElseInitPromise(refId)

          // prepare builder
          inputs match {
            case List()  =>
              promise.failure(new NoSuchElementException("no input silo"))
            case x :: xs =>
              println(s"NODE $refId: creating builder")
              val builder = x.bf.mkBuilder()
              // add mapping for emitterId
              // builder, num completed, required completed
              val triple = (builder, 0, inputs.size)
              builderOfEmitterId += (emitterId -> triple)
              // send DoPumpTo messages to inputs
              inputs.foreach { case input: PumpNodeInput[u, v, r, p] =>
                val host = system.location(input.from.refId)
                val inputNodeActor = Config.m(host)
                // must also send node (input.from), so that the input silo can be completed first
                println(s"NODE $refId: sending DoPumpTo to ${input.from.refId}")
                inputNodeActor ! DoPumpTo[u, v, p](input.from, input.fun.asInstanceOf[p], input.pickler, input.unpickler, emitterId, system.location(refId), refId)
              }
              // register completion for responding with ForceResponse
              promise.future.foreach { (silo: LocalSilo[_, _]) =>
                s ! ForceResponse(silo.value)
              }
          }
      }

    // case class DoPumpTo[A, B](node: Node, fun: (A, Emitter[B]) => Unit, emitterId: Int, destHost: Host, destRefId: Int)
    case pump: DoPumpTo[a, b, p] =>
      val node      = pump.node
      val fun       = pump.fun
      val emitterId = pump.emitterId
      val destHost  = pump.destHost // currently unused, but might be useful for Netty backend
      val destRefId = pump.destRefId

      println(s"NODE ${node.refId}: received DoPumpTo")

      val emitter = new RemoteEmitter[b](sender, emitterId, destRefId)

      print(s"NODE ${node.refId}: getOrElseInitPromise... ")
      val promise = getOrElseInitPromise(node.refId)
      promise.future.foreach { localSilo =>
        // println(s"SERVER: calling doPumpTo on local Silo (${localSilo.value})")
        localSilo.doPumpTo[a, b](fun.asInstanceOf[(a, Emitter[b]) => Unit], emitter)
      }

      // kick off materialization
      // println(s"NODE ${node.refId}: kick off materialization by sending Graph($node)")
      self ! Graph(node)

    /*
     *  @tparam a old element type
     *  @tparam b new element type
     *  @tparam c new collection type
     */
    case pumpTo: PumpTo[a, b, c] =>
      val emitterId   = pumpTo.emitterId
      val srcRefId    = pumpTo.srcRefId
      val destHost    = pumpTo.destHost
      val destRefId   = pumpTo.destRefId
      val fun         = pumpTo.fun
      val bf          = pumpTo.bf
      println(s"SERVER: initiating pumpTo from $srcRefId to $destRefId @ $destHost...")

      // at this point, we know:
      //   fun: (a, Emitter[b]) => Unit
      //   bf: BuilderFactory[a, b]

      val fut: Future[Emitter[b]] =
        mkEmitter[b, c](emitterId, destHost, destRefId, bf.mkBuilder())

      val localSiloFut = promiseOf(srcRefId).future

      fut.zip(localSiloFut).foreach { case (emitter, localSilo) =>
        // println(s"SERVER: calling doPumpTo on local Silo (${localSilo.value})")
        localSilo.doPumpTo[a, b](fun, emitter)
      }

    case CreatSilo(emitterId, destRefId, builder) =>
      println(s"node actor ${self.path.name} received CreateSilo msg")
      // builderOfEmitterId += (emitterId -> builder)
      val triple = (builder, 0, 1)
      builderOfEmitterId += (emitterId -> triple)
      sender ! "Done"

    case Emit(emitterId, destRefId, ba) =>
      // println(s"node actor ${self.path.name} received Emit msg")
      builderOfEmitterId.get(emitterId) match {
        case None => ???
        case Some((builder, current, required)) =>
          val pickle = BinaryPickleArray(ba.asInstanceOf[Array[Byte]])
            // JSONPickle(ba.asInstanceOf[String])
          val sdv = pickle.unpickle[SelfDescribing] // *static* unpickling
          // val v = sdv.result()
          val pickle0 = BinaryPickleArray(sdv.blob)
          val v = pickle0.unpickle[Any] // runtime unpickling
          // println(s"received ${v.toString}")
          val stableBuilder = builder
          stableBuilder += v.asInstanceOf[stableBuilder.Elem]
      }

    case Done(emitterId, destRefId) =>
      builderOfEmitterId.get(emitterId) match {
        case None => ???
        case Some((builder, current, required)) =>
          if (current == required - 1) {
            // we're done, last emitter finished

            // here we need to call result() on the builder
            // but it would be better to directly create a local Silo
            val newSilo = builder.resultSilo()

            // complete promise for destRefId, so that force calls can complete
            promiseOf(destRefId).success(newSilo)

            // println(s"SERVER: created silo containing: ${newSilo.value}")
          } else {
            val newTriple = (builder, current + 1, required)
            builderOfEmitterId += (emitterId -> newTriple)
          }
      }
  }
}
