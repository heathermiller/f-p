package silt
package actors

import scala.pickling._

import akka.actor.{Actor, ActorSystem, Props, Terminated}
import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import java.util.concurrent.atomic.AtomicInteger

import graph._


sealed trait ActorMessage

case class Terminate() extends ActorMessage

case class StartNodeActors(val actorsNumbers: Int) extends ActorMessage

case class NodeActorsStarted() extends ActorMessage

case class ChangePort(val port: Int) extends ActorMessage

/** Supervisor of all NodeActors.
 */
class SystemActor(system: SiloSystemInternal) extends Actor {

  var port = 8090

  val waitingForTermination = Promise[Boolean]()
  var started = false

  def receive = {
    case ChangePort(n) =>
      port = n
    case StartNodeActors(n) =>
      if (started) sender ! NodeActorsStarted()
      else {
        // create node actors, put into config
        for (i <- 0 to n) {
          val nodeActor = context.actorOf(Props(new NodeActor(system)))
          context.watch(nodeActor)
          Config.m += (Host("127.0.0.1", port + i) -> nodeActor)
          println(s"added node actor ${nodeActor.path}")
        }
        started = true
        sender ! NodeActorsStarted()
      }

    case Terminate() => // stop all NodeActors
      val s = sender
      println("Terminating..")
      waitingForTermination.future.foreach(x => s ! "Done")
      Config.m.values.foreach { nodeActor =>
        println(s"stopping node actor ${nodeActor.path.name}")
        nodeActor ! Terminate()
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

class SystemImpl extends SiloSystem with SiloSystemInternal {

  private val actorSystem = ActorSystem("silo-system")

  // the system actor creates a few node actors
  private val systemActor = actorSystem.actorOf(Props(new SystemActor(this)))

  private implicit val timeout: Timeout = 300.seconds

  def start(): Future[Boolean] = {
    start(3)
  }

  def start(numAct: Int): Future[Boolean] = {
    (systemActor ? StartNodeActors(numAct)).map { x => true }
  }

  def changePort(port: Int): Unit = {
    systemActor ? ChangePort(port)
  }

  def initRequest[T, V <: ReplyMessage : Pickler](host: Host, mkMsg: Int => V): Future[SiloRef[T]] = {
    (systemActor ? StartNodeActors(3)).flatMap { x =>
      val nodeActor    = Config.m(host)
      val refId        = refIds.incrementAndGet()
      val initSilo     = mkMsg(refId)
      initSilo.id      = seqNum.incrementAndGet()
      location += (refId -> host)
      (nodeActor ? initSilo).map { x =>
        println("SystemImpl: got response for InitSilo msg")
        // create a typed wrapper
        new MaterializedSiloRef[T](refId, host)(this)
      }
    }
  }

  def waitUntilAllClosed(): Unit = {
    val tm = implicitly[Timeout]
    waitUntilAllClosed(tm.duration, tm.duration)
  }

  def waitUntilAllClosed(nodeTimeout: FiniteDuration, systemTimeout: FiniteDuration): Unit = {
    val done = (systemActor ? Terminate())(Timeout(nodeTimeout))
    Await.ready(done, systemTimeout)
    actorSystem.shutdown()
  }

  def send[T <: ReplyMessage : Pickler](host: Host, msg: T): Future[Any] = {
    val nodeActor = Config.m(host)
    println(s"found node actor ${nodeActor.path.name}")
    (nodeActor ? msg).map {
      case ForceResponse(value) => value
      case OKCreated(_) => }
  }
}
