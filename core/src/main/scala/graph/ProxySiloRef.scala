package silt
package graph

import scala.spores._
import scala.pickling._
import Defaults._
import binary._

import SporePickler._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import Picklers._


final case class PumpToInput[T <: Traversable[U], U, V, R <: Traversable[_], P <: Spore2[U, Emitter[V], Unit]](from: ProxySiloRef[U, T], fun: P, pickler: Pickler[P], unpickler: Unpickler[P], bf: BuilderFactory[V, R])

// IDEA: create ref ids immediately, as well as emitter ids
abstract class ProxySiloRef[W, T <: Traversable[W]](refId: Int, val host: Host)(implicit system: SiloSystemInternal) extends SiloRef[W, T] {

  override def apply[V, S <: Traversable[V]](g: Spore[T, S])
                                    (implicit pickler: Pickler[Spore[T, S]], unpickler: Unpickler[Spore[T, S]]): SiloRef[V, S] = {
    val newRefId = system.refIds.incrementAndGet()
    val host = system.location(refId)
    println(s"apply: register location of $newRefId: $host")
    system.location += (newRefId -> host)
    new ApplySiloRef[W, T, V, S](this, newRefId, g, pickler, unpickler)
  }

  override def pumpTo[V, R <: Traversable[V], P <: Spore2[W, Emitter[V], Unit]](destSilo: SiloRef[V, R])(fun: P)
                                             (implicit bf: BuilderFactory[V, R], pickler: Pickler[P], unpickler: Unpickler[P]): Unit = {
    // register `this` SiloRef as pump input for `destSilo`
    destSilo.asInstanceOf[ProxySiloRef[V, R]].addInput(PumpToInput(this, fun, pickler, unpickler, bf))
  }

  private def materialize(cache: Boolean): Future[T] = {
    // 1. build graph
    val n = node()
    // 2. send graph to node which contains `this` Silo
    print(s"looking up refId $refId...")
    val host = system.location(refId)
    println(s" $host")
    system.send(host, Graph(n, cache)).map(_.asInstanceOf[T])
  }

  def send(): Future[T] = {
    materialize(false)
  }

  def cache(): SiloRef[W, T] = {
    materialize(true)
    this
  }

  override def flatMap[V, S <: Traversable[V]](fun: Spore[T, SiloRef[V, S]])
    (implicit pickler: Pickler[Spore[T, SiloRef[V, S]]],
      unpickler: Unpickler[Spore[T, SiloRef[V, S]]]): SiloRef[V, S] = {
    val newRefId = system.refIds.incrementAndGet()
    val host = system.location(refId)
    system.location += (newRefId -> host)
    new FMappedSiloRef(this, newRefId, fun, pickler, unpickler)
  }

  def id = SiloRefId(refId)

  def node(): Node

  protected var inputs = List[PumpToInput[_, _, _, T, _]]()

  def addInput[S <: Traversable[U], U, V](input: PumpToInput[S, U, V, T, _]): Unit = {
    inputs ::= input
  }

}

class ApplySiloRef[V, S <: Traversable[V], U, T <: Traversable[U]]
  (val input: ProxySiloRef[V, S], val refId: Int, val f: Spore[S, T],
                   val pickler: Pickler[Spore[S, T]], val unpickler: Unpickler[Spore[S, T]])
  (implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId, input.host) { // result on same host as input
  def node(): Node = {
    // recursively create graph node for `input`
    val prevNode = input.node()
    new Apply[V, S, U, T](prevNode, refId, f, pickler, unpickler)
  }
}

class FMappedSiloRef[V, S <: Traversable[V], U, T <: Traversable[U]]
                    (val input: ProxySiloRef[V, S], val refId: Int, val f: Spore[S, SiloRef[U, T]],
                     val pickler: Pickler[Spore[S, SiloRef[U, T]]], val unpickler: Unpickler[Spore[S, SiloRef[U, T]]])
  (implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId, input.host) { // result on same host as input
  def node(): Node = {
    // recursively create graph node for `input`
    val prevNode = input.node()

    new FMapped[V, S, U, T](prevNode, refId, f, pickler, unpickler)
  }
}

// created by SystemImpl.fromClass
class MaterializedSiloRef[U, T <: Traversable[U]](val refId: Int, host: Host)(implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId, host) {
  def node(): Node = {
    new Materialized(refId)
  }
}

// created by SystemImpl.emptySilo
class EmptySiloRef[U, T <: Traversable[U]](val refId: Int, host: Host)(implicit system: SiloSystemInternal) extends ProxySiloRef[U, T](refId, host) {
  val emitterId = system.emitterIds.incrementAndGet()
  def node(): Node = {
    val nodeInputs = inputs.map { case pumpToInput: PumpToInput[s, u, v, T, p] =>
      val fromNode = pumpToInput.from.node()
      println(s"empty silo $refId has input ${fromNode.refId}")
      PumpNodeInput[u, v, T, p](fromNode, pumpToInput.from.host, pumpToInput.fun.asInstanceOf[p], pumpToInput.pickler.asInstanceOf[Pickler[p]], pumpToInput.unpickler.asInstanceOf[Unpickler[p]], pumpToInput.bf)
    }
    new MultiInput(nodeInputs, refId, host, emitterId)
  }
}
