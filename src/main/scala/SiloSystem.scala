package silt

import scala.pickling._
import Defaults._

import scala.concurrent.Future
import scala.collection.mutable
import scala.collection.concurrent.TrieMap

import java.util.concurrent.atomic.AtomicInteger

import silt.graph.EmptySiloRef


trait SiloSystem {
  self: SiloSystemInternal =>

  def fromClass[U, T <: Traversable[U]](clazz: Class[_], host: Host): Future[SiloRef[U, T]] =
    initRequest[U, T, InitSilo](host, { (refId: Int) =>
      println(s"fromClass: register location of $refId")
      InitSilo(clazz.getName(), refId)
    })

  def fromFun[U, T <: Traversable[U]](host: Host)(fun: () => LocalSilo[U, T])(implicit p: Pickler[InitSiloFun[U, T]]): Future[SiloRef[U, T]] =
    initRequest[U, T, InitSiloFun[U, T]](host, { (refId: Int) =>
      println(s"fromFun: register location of $refId")
      InitSiloFun(fun, refId)
    })

  // the idea is that empty silos can only be filled using pumpTos.
  // we'll detect issues like trying to fill a silo that's been constructed
  // *not* using pumpTo at runtime (before running the topology, though!)
  //
  // to push checking further to compile time, could think of
  // using phantom types to detect whether a ref has been target of non-pumpTo!
  def emptySilo[U, T <: Traversable[U]](host: Host): SiloRef[U, T] = {
    val refId = refIds.incrementAndGet()
    location += (refId -> host)
    new EmptySiloRef[U, T](refId, host)(this)
  }

  def waitUntilAllClosed(): Unit
}

// abstracts from different network layer backends
private[silt] trait SiloSystemInternal {

  // map ref ids to host locations
  val location: mutable.Map[Int, Host] = new TrieMap[Int, Host]

  val seqNum = new AtomicInteger(10)

  val refIds = new AtomicInteger(0)

  val emitterIds = new AtomicInteger(0)

  // idea: return Future[SelfDescribing]
  def send[T <: ReplyMessage : Pickler](host: Host, msg: T): Future[Any]

  def initRequest[U, T <: Traversable[U], V <: ReplyMessage : Pickler](host: Host, mkMsg: Int => V): Future[SiloRef[U, T]]

}
