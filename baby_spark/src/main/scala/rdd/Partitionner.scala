package baby_spark.rdd

import scala.collection.generic.CanBuildFrom
import silt.{ SiloRef }

import scala.spores._
import scala.pickling._
import SporePickler._

sealed trait PartitionState

object State {
  sealed trait NotPartitionned extends PartitionState
  sealed trait Partitionned extends PartitionState
}

trait Partitionner[St <: PartitionState, V, S <: Traversable[V]] {
  self =>
  def silos: Seq[SiloRef[S]]
  // TODO: This doesn't require the Partitionner to be of the same type as before: FIX THIS
  def applyPartition(implicit ev: St =:= State.NotPartitionned): Partitionner[State.Partitionned, V, S]
  def getPartition(n: Int)(implicit ev: St =:= State.Partitionned): Option[SiloRef[S]]

  def getPartition(silo: SiloRef[S])(implicit ev: St =:= State.Partitionned): Option[Int]
}

class NullPartitionner[V, S <: Traversable[V]](val silos: Seq[SiloRef[S]]) extends Partitionner[State.Partitionned, V, S] {
  override def applyPartition(implicit ev: State.Partitionned =:= State.NotPartitionned): NullPartitionner[V, S] = {
    new NullPartitionner[V, S](silos)
  }

  override def getPartition(n: Int)(implicit ev: State.Partitionned =:= State.Partitionned): Option[SiloRef[S]] = None
  override def getPartition(silo: SiloRef[S])(implicit ev: State.Partitionned =:= State.Partitionned): Option[Int] = None
}

object HashPartionner {
  // XXX: Change the name, or rename the other getPartition function
  def getPartition(numPartitions: Int)(x: Any): Int = Math.floorMod(x.hashCode(), numPartitions)
}

// XXX: Probably to modify: instead of having the partitionning in this, which
// would probably be replicated in others, do it in a partitionBy function,
// inside the RDD, and have the Partitionner simply be a function from Any to
// Int, that you apply in a filter (== partF below).
// Problem to take care of: how to deal with two RDD with the same partitionner,
//to take advantage of it.
class HashPartionner[St <: PartitionState, V, S <: Traversable[V]](val silos: Seq[SiloRef[S]], val numPartitions: Int)(implicit cbf: CanBuildFrom[S, V, S]) extends Partitionner[St, V, S] {

  override def applyPartition
    (implicit ev: St =:= State.NotPartitionned): HashPartionner[State.Partitionned, V, S] = {

    def shuffleSilo(silo1: SiloRef[S], silo2: SiloRef[S], partition: Int): SiloRef[S] = {

      def partF(x: Any): Boolean = HashPartionner.getPartition(numPartitions)(x) == partition

      silo2.flatMap(spore {
        val ls1 = silo1
        val lPartF = partF _
        val lcbf = cbf
        s2 => {
          ls1.apply(spore {
            val ls2 = s2.filter(lPartF(_))
            val llPartF = lPartF
            val llcbf = lcbf
            s1 => {
              val builder = llcbf()
              builder ++= s1.filter(llPartF)
              builder ++= ls2
              builder.result()
            }
          })
        }
      })
    }

    val newSilos = silos.zipWithIndex.map { case (s1, index) =>
      silos.filter(_ != s1).fold(s1) {
        (currentSilo, otherSilo) => shuffleSilo(currentSilo, otherSilo, index)
      }
    }

    new HashPartionner[State.Partitionned, V, S](newSilos, numPartitions)
  }

  override def getPartition(n: Int)(implicit ev: St =:= State.Partitionned): Option[SiloRef[S]] = {
    if (n >= 0 && n < numPartitions) Some(silos(n)) else None
  }

  override def getPartition(silo: SiloRef[S])(implicit ev: St =:= State.Partitionned): Option[Int] = {
    silos.indexOf(silo) match {
      case -1 => None
      case x => Some(x)
    }
  }
}
