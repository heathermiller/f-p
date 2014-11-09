package silt

import scala.pickling._
import Defaults._

import scala.concurrent.Future
import scala.collection.mutable
import scala.collection.concurrent.TrieMap

import java.util.concurrent.atomic.AtomicInteger


trait SiloSystem {

  def fromClass[U, T <: Traversable[U]](clazz: Class[_], host: Host): Future[SiloRef[U, T]]

  def waitUntilAllClosed(): Unit

  def seqNum: AtomicInteger

  def refIds: AtomicInteger

  def emitterIds: AtomicInteger

}

// abstracts from different network layer backends
private[silt] trait SiloSystemInternal extends SiloSystem {

  // map ref ids to host locations
  val location: mutable.Map[Int, Host] = new TrieMap[Int, Host]

  // idea: return Future[SelfDescribing]
  def send[T <: ReplyMessage : Pickler](host: Host, msg: T): Future[Any]

}
