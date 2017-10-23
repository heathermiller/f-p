package silt
package graph

import scala.spores._
import scala.pickling._
import Defaults._
import binary._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import Picklers._


final case class PumpToInput[T, U, V, R, P <: Spore2[U, Emitter[V], Unit]](from: ProxySiloRef[T], fun: P, pickler: Pickler[P], unpickler: Unpickler[P], bf: BuilderFactory[V, R])

// IDEA: create ref ids immediately, as well as emitter ids
abstract class ProxySiloRef[T](refId: Int, val host: Host)(implicit system: SiloSystemInternal) extends SiloRef[T] {

  override def map[S](g: Spore[T, S])
                     (implicit pickler: Pickler[Spore[T, S]], unpickler: Unpickler[Spore[T, S]]): SiloRef[S] = {
    val newRefId = system.refIds.incrementAndGet()
    val host = system.location(refId)
    println(s"apply: register location of $newRefId: $host")
    system.location += (newRefId -> host)
    new ApplySiloRef[T, S](this, newRefId, g, pickler, unpickler)
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
    val n = node()
    val host = system.location(refId)
    system.send(host, Graph(n, false)).map(_.asInstanceOf[T])
  }

  def cache(): Future[SiloRef[T]] = {
    val n = node()
    val host = system.location(refId)
    system.send(host, Graph(n, true)).map(x => this)
  }

  override def apply[S](fun: Spore[T, SiloRef[S]])
    (implicit pickler: Pickler[Spore[T, SiloRef[S]]],
      unpickler: Unpickler[Spore[T, SiloRef[S]]]): SiloRef[S] = {
    val newRefId = system.refIds.incrementAndGet()
    val host = system.location(refId)
    system.location += (newRefId -> host)
    new FMappedSiloRef(this, newRefId, fun, pickler, unpickler)
  }

  def id = SiloRefId(refId)

  def node(): Node

  protected var inputs = List[PumpToInput[_, _, _, T, _]]()

  def addInput[S, U, V](input: PumpToInput[S, U, V, T, _]): Unit = {
    inputs ::= input
  }

  def elems[W](implicit ev: T <:< Traversable[W]): ElemsProxySiloRef[W, T] =
    new ElemsProxySiloRef[W, T](this)
}

class ElemsProxySiloRef[W, T](ref: ProxySiloRef[T]) extends ElemsSiloRef[W, T] {

  override def pumpTo[V, R <: Traversable[V], P <: Spore2[W, Emitter[V], Unit]](destSilo: SiloRef[R])(fun: P)
    (implicit bf: BuilderFactory[V, R], pickler: Pickler[P], unpickler: Unpickler[P]): Unit = {
    // register `this` SiloRef as pump input for `destSilo`
    destSilo.asInstanceOf[ProxySiloRef[R]].addInput[T, W, V](PumpToInput(ref, fun, pickler, unpickler, bf))
  }
}

class ApplySiloRef[S, T]
  (val input: ProxySiloRef[S], val refId: Int, val f: Spore[S, T],
                   val pickler: Pickler[Spore[S, T]], val unpickler: Unpickler[Spore[S, T]])
  (implicit system: SiloSystemInternal) extends ProxySiloRef[T](refId, input.host) { // result on same host as input
  def node(): Node = {
    // recursively create graph node for `input`
    val prevNode = input.node()
    new Apply[S, T](prevNode, refId, f, pickler, unpickler)
  }
}

class FMappedSiloRef[S, T]
                    (val input: ProxySiloRef[S], val refId: Int, val f: Spore[S, SiloRef[T]],
                     val pickler: Pickler[Spore[S, SiloRef[T]]], val unpickler: Unpickler[Spore[S, SiloRef[T]]])
  (implicit system: SiloSystemInternal) extends ProxySiloRef[T](refId, input.host) { // result on same host as input
  def node(): Node = {
    // recursively create graph node for `input`
    val prevNode = input.node()
    new FMapped[S, T](prevNode, refId, f, pickler, unpickler)
  }
}

// created by SystemImpl.fromClass
class MaterializedSiloRef[T](val refId: Int, host: Host)(implicit system: SiloSystemInternal) extends ProxySiloRef[T](refId, host) {
  def node(): Node = {
    new Materialized(refId)
  }
}

// created by SystemImpl.emptySilo
class EmptySiloRef[T](val refId: Int, host: Host)(implicit system: SiloSystemInternal) extends ProxySiloRef[T](refId, host) {
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
