package silt
package graph

import scala.spores._
import scala.pickling._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


final case class PumpToInput[T <: Traversable[U], U, V, R <: Traversable[_]](from: ProxySiloRef[U, T], fun: (U, Emitter[V]) => Unit, bf: BuilderFactory[V, R])

// IDEA: create ref ids immediately, as well as emitter ids
abstract class ProxySiloRef[W, T <: Traversable[W]](refId: Int)(implicit system: SiloSystemInternal) extends SiloRef[W, T] {

  def apply[V, S <: Traversable[V]](g: Spore[T, S]): SiloRef[V, S] = {
    val newRefId = system.refIds.incrementAndGet()
    val host = system.location(refId)
    system.location += (newRefId -> host)
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
    val host = system.location(refId)
    println(s" $host")
    system.send(host, Graph(n)).map(_.asInstanceOf[T])
  }

  def id = SiloRefId(refId)

  def node(): Node

  protected var inputs = List[PumpToInput[_, _, _, T]]()

  def addInput[S <: Traversable[U], U, V](input: PumpToInput[S, U, V, T]): Unit = {
    inputs ::= input
  }

}

class ApplySiloRef[V, S <: Traversable[V], U, T <: Traversable[U]](val input: ProxySiloRef[V, S], val refId: Int, val f: S => T)(implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId) {
  def node(): Node = {
    // recursively create graph node for `input`
    val prevNode = input.node()
    new Apply[V, S, U, T](prevNode, refId, f)
  }
}

// created by SystemImpl.fromClass
class MaterializedSiloRef[U, T <: Traversable[U]](val refId: Int)(implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId) {
  def node(): Node = {
    new Materialized(refId)
  }
}

// created by SystemImpl.emptySilo
class EmptySiloRef[U, T <: Traversable[U]](val refId: Int)(implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId) {
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
