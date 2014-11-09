package silt
package actors

import scala.pickling._
import Defaults._
import binary._

import akka.actor.{Actor, ActorSystem, Props, Terminated}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import java.util.concurrent.atomic.AtomicInteger

import graph._


case object Terminate

case object StartNodeActors

case object NodeActorsStarted

/** Supervisor of all NodeActors.
 */
class SystemActor(system: SiloSystemInternal) extends Actor {

  val waitingForTermination = Promise[Boolean]()
  var started = false

  def receive = {
    case StartNodeActors =>
      if (started) sender ! NodeActorsStarted
      else {
        // create node actors, put into config
        for (i <- 0 to 3) {
          val nodeActor = context.actorOf(Props(new NodeActor(system)))
          context.watch(nodeActor)
          Config.m += (Host("127.0.0.1", 8090 + i) -> nodeActor)
          println(s"added node actor ${nodeActor.path.name}")
        }
        started = true
        sender ! NodeActorsStarted
      }

    case Terminate => // stop all NodeActors
      val s = sender
      waitingForTermination.future.foreach(x => s ! "Done")
      Config.m.values.foreach { nodeActor =>
        println(s"stopping node actor ${nodeActor.path.name}")
        nodeActor ! Terminate
      }

    case Terminated(from) =>
      Config.m.find(_._2 == from) match {
        case None =>
          assert(false, "unregistered node actor terminated")
        case Some((id, _)) =>
          println(s"removing node actor ${from.path.name}")
          Config.m -= id
          if (Config.m.size == 0) {
            println("all node actors terminated")
            waitingForTermination.success(true)
            context.stop(self)
          }
      }      
  }
}

class SystemImpl extends SiloSystemInternal {

  val seqNum = new AtomicInteger(10)

  val refIds = new AtomicInteger(0)

  val emitterIds = new AtomicInteger(0)

  private val actorSystem = ActorSystem("silo-system")

  // the system actor creates a few node actors
  private val systemActor = actorSystem.actorOf(Props(new SystemActor(this)))

  private implicit val timeout: Timeout = 30.seconds

  def start(): Future[Boolean] = {
    (systemActor ? StartNodeActors).map { x => true }
  }

  def fromFun[U, T <: Traversable[U]](host: Host)(fun: () => LocalSilo[U, T]): Future[SiloRef[U, T]] = {
    (systemActor ? StartNodeActors).flatMap { x =>
      val nodeActor    = Config.m(host)
      val refId        = refIds.incrementAndGet()
      println(s"fromClass: register location of $refId")
      location += (refId -> host)
      val initSilo     = InitSiloFun(fun, refId)
      initSilo.id      = seqNum.incrementAndGet()
      (nodeActor ? initSilo).map { x =>
        println("SystemImpl: got response for InitSilo msg")
        // create a typed wrapper
        new MaterializedSiloRef[U, T](refId)(this)
      }
    }
  }

  def fromClass[U, T <: Traversable[U]](clazz: Class[_], host: Host): Future[SiloRef[U, T]] = {
    (systemActor ? StartNodeActors).flatMap { x =>
      val refId = refIds.incrementAndGet()
      println(s"fromClass: register location of $refId")
      location += (refId -> host)
      val initSilo  = InitSilo(clazz.getName(), refId)
      initSilo.id   = seqNum.incrementAndGet()
      val nodeActor = Config.m(host)
      (nodeActor ? initSilo).map { x =>
        println("SystemImpl: got response for InitSilo msg")
        // create a typed wrapper
        new MaterializedSiloRef[U, T](refId)(this)
      }
    }
  }

  // the idea is that empty silos can only be filled using pumpTos.
  // we'll detect issues like trying to fill a silo that's been constructed
  // *not* using pumpTo at runtime (before running the topology, though!)
  //
  // to push checking further to compile time, could think of
  // using phantom types to detect whether a ref has been target of non-pumpTo!
  def emptySilo[U, T <: Traversable[U]](host: Host): SiloRef[U, T] = {
    val refId = refIds.incrementAndGet()
    location += (refId -> host)
    new EmptySiloRef[U, T](refId)(this)
  }

  def waitUntilAllClosed(): Unit = {
    val done = systemActor ? Terminate
    Await.ready(done, 30.seconds)
    actorSystem.shutdown()
  }

  def send[T <: ReplyMessage : Pickler](host: Host, msg: T): Future[Any] = {
    val nodeActor = Config.m(host)
    println(s"found node actor ${nodeActor.path.name}")
    (nodeActor ? msg).map { case ForceResponse(value) => value }
  }
}
