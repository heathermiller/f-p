package baby_spark
package rdd

import scala.reflect.runtime.universe._

import scala.collection.generic.CanBuildFrom
import silt.{ SiloRef }

trait Partitioner[T] {
  def numPartitions: Int
  def getPartition(k: T): Int
}

class ProxyPartitioner[T, A](val transformFunc: T => A, val otherPartitioner: Partitioner[A]) extends Partitioner[T] {
  def numPartitions: Int = otherPartitioner.numPartitions
  def getPartition(k: T): Int = otherPartitioner.getPartition(transformFunc(k))
}

class HashPartitioner[T: TypeTag](val numPartitions: Int) extends Partitioner[T] {

  val tt = typeTag[T]

  def getPartition(k: T): Int = Math.floorMod(k.hashCode(), numPartitions)

  override def equals(other: Any): Boolean = other match {
    case e: HashPartitioner[_] if e.tt == this.tt => e.numPartitions == numPartitions
    case _ => false
  }
}

case class KeyPartitioner[K: TypeTag, V](override val numPartitions: Int) extends ProxyPartitioner[(K, V), K]((tup: (K, V)) => tup._1, new HashPartitioner(numPartitions)) {

  val tt = typeTag[K]

  override def equals(other: Any): Boolean = other match {
    case e: KeyPartitioner[_, _] if e.tt == this.tt => e.numPartitions == numPartitions
    case _ => false
  }
}
