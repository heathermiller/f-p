package silt
package actors

import scala.spores._

import scala.pickling._
import binary._

import akka.util.Timeout
import akka.pattern.ask

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


/** A node in the computation graph.
 *
 *  We must be able to generate a pickler for it.
 */
sealed abstract class Node {
  def refId: Int
}

final class Materialized(val refId: Int) extends Node

final class Apply[U, T <: Traversable[U], V, S <: Traversable[V]](val input: Node, val refId: Int, val fun: T => S) extends Node

final class MultiInput[R](val inputs: List[PumpNodeInput[_, _, R]], val refId: Int, val emitterId: Int) extends Node

final case class PumpNodeInput[U, V, R](from: Node, fun: (U, Emitter[V]) => Unit, bf: BuilderFactory[V, R])


final case class PumpToInput[T <: Traversable[U], U, V, R <: Traversable[_]](from: ProxySiloRef[U, T], fun: (U, Emitter[V]) => Unit, bf: BuilderFactory[V, R])

// IDEA: create ref ids immediately, as well as emitter ids
abstract class ProxySiloRef[W, T <: Traversable[W]](refId: Int)(implicit system: SiloSystem) extends SiloRef[W, T] {

  implicit val timeout: Timeout = 30.seconds

  def apply[V, S <: Traversable[V]](g: Spore[T, S]): SiloRef[V, S] = {
    val newRefId = system.refIds.incrementAndGet()
    val host = Config.location(refId)
    Config.location += (newRefId -> host)
    new ApplySiloRef[W, T, V, S](this, newRefId, g)
  }

  override def pumpTo[V, R <: Traversable[V]](destSilo: SiloRef[V, R])(fun: Spore2[W, Emitter[V], Unit])
                                             (implicit bf: BuilderFactory[V, R], pickler: SPickler[V], unpickler: Unpickler[V]): Unit = {
    // register `this` SiloRef as pump input for `destSilo`
    destSilo.asInstanceOf[ProxySiloRef[V, R]].addInput(PumpToInput(this, fun, bf))
  }

  def send(): Future[T] = {
    // 1. build graph
    val n = node()
    // 2. send graph to node which contains `this` Silo
    print(s"looking up refId $refId...")
    val host = Config.location(refId)
    println(s" $host")
    val nodeActor = Config.m(host)
    println(s"found node actor ${nodeActor.path.name}")
    (nodeActor ? Graph(n)).map { case ForceResponse(value) => value.asInstanceOf[T] }
  }

  def id = SiloRefId(refId)

  def node(): Node

  protected var inputs = List[PumpToInput[_, _, _, T]]()

  def addInput[S <: Traversable[U], U, V](input: PumpToInput[S, U, V, T]): Unit = {
    inputs ::= input
  }

}

class ApplySiloRef[V, S <: Traversable[V], U, T <: Traversable[U]](val input: ProxySiloRef[V, S], val refId: Int, val f: S => T)(implicit system: SiloSystem) extends ProxySiloRef[U, T](refId) {
  def node(): Node = {
    // recursively create graph node for `input`
    val prevNode = input.node()
    new Apply[V, S, U, T](prevNode, refId, f)
  }
}

// created by SystemImpl.fromClass
class MaterializedSiloRef[U, T <: Traversable[U]](val refId: Int)(implicit system: SiloSystem) extends ProxySiloRef[U, T](refId) {
  def node(): Node = {
    new Materialized(refId)
  }
}

// created by SystemImpl.emptySilo
class EmptySiloRef[U, T <: Traversable[U]](val refId: Int)(implicit system: SiloSystem) extends ProxySiloRef[U, T](refId) {
  val emitterId = system.emitterIds.incrementAndGet()
  def node(): Node = {
    val nodeInputs = inputs.map { case pumpToInput: PumpToInput[s, u, v, T] =>
      val fromNode = pumpToInput.from.node()
      println(s"empty silo $refId has input ${fromNode.refId}")
      PumpNodeInput[u, v, T](fromNode, pumpToInput.fun, pumpToInput.bf)
    }
    new MultiInput(nodeInputs, refId, emitterId)
  }
}
