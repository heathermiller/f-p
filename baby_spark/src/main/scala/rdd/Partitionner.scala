package baby_spark.rdd

import scala.collection.generic.CanBuildFrom
import silt.{ SiloRef }

trait Partitioner[T] {
  def numPartitions: Int
  def getPartition(k: T): Int
}

class ProxyPartitioner[T, A](val transformFunc: T => A, val otherPartitioner: Partitioner[A]) extends Partitioner[T] {
  def numPartitions: Int = otherPartitioner.numPartitions
  def getPartition(k: T): Int = otherPartitioner.getPartition(transformFunc(k))

  override def equals(other: Any): Boolean = other match {
    case e: ProxyPartitioner[T, A] => {
      e.otherPartitioner == otherPartitioner
    }
    case _ => false
  }
}

class HashPartitioner[T](val numPartitions: Int) extends Partitioner[T] {

  def getPartition(k: T): Int = Math.floorMod(k.hashCode(), numPartitions)

  override def equals(other: Any): Boolean = other match {
    case e: HashPartitioner[T] => e.numPartitions == numPartitions
    case _ => false
  }

}
