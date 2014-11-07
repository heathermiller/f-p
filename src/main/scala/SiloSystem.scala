package silt

import scala.concurrent.Future

import java.util.concurrent.atomic.AtomicInteger


trait SiloSystem {

  def fromClass[U, T <: Traversable[U]](clazz: Class[_], host: Host): Future[SiloRef[U, T]]

  def waitUntilAllClosed(): Unit

  def seqNum: AtomicInteger

  def refIds: AtomicInteger

  def emitterIds: AtomicInteger

}
