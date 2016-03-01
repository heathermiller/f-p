package baby_spark
package rdd

import scala.concurrent.{ Await, ExecutionContext, Future }
import scala.concurrent.duration.Duration

import scalaz._
import Scalaz._

import silt.SiloRef


class MapSemigroupRDD[K, V : Semigroup, S <: Traversable[(K, V)] : Semigroup](override val silos: Seq[SiloRef[(K, V), S]]) extends MapRDD[K, V, S](silos) {

  def collectMap()(implicit ec: ExecutionContext): S = {
    Await.result(Future.sequence(silos.map {
      s => s.send()
    }), Duration.Inf).reduce((a, b) => a |+| b)
  }
}
