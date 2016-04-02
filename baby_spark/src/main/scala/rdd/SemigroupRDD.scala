package baby_spark
package rdd

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration

import scalaz._
import Scalaz._
import silt.{ Host, SiloRef }

object MapSemigroupRDD {
  def apply[K, V : Semigroup, S <: Traversable[(K, V)] : Semigroup](
    silos: Seq[SiloRef[S]], hosts: Seq[Host]): MapSemigroupRDD[K, V, S] = {
  new MapSemigroupRDD(silos, hosts, new NullPartitionner(silos))
}

}

class MapSemigroupRDD[K, V : Semigroup, S <: Traversable[(K, V)] : Semigroup](
  override val silos: Seq[SiloRef[S]],
  override val hosts: Seq[Host],
override val partitionner: Partitionner[_, (K, V), S]) extends MapRDD[K, V,
  S](silos, hosts, partitionner) {

  def collectMap()(implicit ec: ExecutionContext): S = {
    Await.result(Future.sequence(silos.map {
      s => s.send()
    }), Duration.Inf).reduce((a, b) => a |+| b)
  }
}
