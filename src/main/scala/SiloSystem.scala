package silt

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

  def send(host: Host, msg: Any): Future[Any]

}
