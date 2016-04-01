package baby_spark
package rdd

import scala.language.higherKinds

import scala.spores._
import scala.pickling._
import SporePickler._
import Defaults._

import scala.concurrent._

import silt._

class MapRDD[K, V, S <: Traversable[(K, V)]]
  (override val silos: Seq[SiloRef[S]],
  override val hosts: Seq[Host])
    extends RDD[(K, V), S](silos, hosts) {

  def reduceByKey[RS[A, B] <: Traversable[(A, B)]](f: Spore2[V, V, V])
    (implicit cbf1: CanBuildTo[(K, V), RS[K, V]]): MapRDD[K, V, RS[K, V]] = {
    val resList = silos.map {
      s => s.apply[RS[K, V]](spore {
        val func = f
        val lcbf = cbf1
        c => {
          val res0 = c.groupBy(_._1)
          val res1 = res0.map(e => (e._1, e._2.map(_._2)))
          val res2 = res1.map(e => (e._1, e._2.reduce(func)))(collection.breakOut(lcbf))
          res2
        }
      })
    }
    new MapRDD(resList, hosts)
  }

  // IS: Traversable type used to store the value for one key
  // RS: Traversable type used to store the mapping key/value
  def groupByKey[IS[A] <: Traversable[A], RS[A, B] <: Traversable[(A, B)]]()
    (implicit cbf1: CanBuildTo[(K, IS[V]), RS[K, IS[V]]], cbf2: CanBuildTo[V, IS[V]]): MapRDD[K, IS[V], RS[K, IS[V]]] = {
    val resList = silos.map {
      s => s.apply[RS[K, IS[V]]](spore {
        val lcbf = cbf1
        val lcbf2 = cbf2
        c => {
          val res0 = c.groupBy(_._1)
          val res1 = res0.map(e => (e._1, e._2.map(_._2)(collection.breakOut(lcbf2))))(collection.breakOut(lcbf))
          res1
        }
      })
    }
    new MapRDD[K, IS[V], RS[K, IS[V]]](resList, hosts)
  }

  def mapValues[W, RS <: Traversable[(K, W)]](f: Spore[V, W])
    (implicit cbt: CanBuildTo[(K, W), RS]): MapRDD[K, W, RS] = {
    map[(K, W), RS](spore {
      val lf = f
      e => (e._1, lf(e._2))
    })
  }

  def join[IS[A] <: Traversable[A], RS[A, B] <: Traversable[(A, B)]]
    (other: MapRDD[K, V, S])
    (implicit cbf1: CanBuildTo[(K, IS[V]), RS[K, IS[V]]],
      cbf2: CanBuildTo[V, IS[V]]): MapRDD[K, IS[V], RS[K, IS[V]]] = {

    val rdd1 = groupByKey[IS, RS]()
    val rdd2 = other.groupByKey[IS, RS]()
    new MapRDD(rdd1.silos ++ rdd2.silos, hosts)
  }

  def join[W, S2 <: Traversable[(K, W)], FS <: Traversable[(K, (V, W))]]
    (other: MapRDD[K, W, S2])
    (implicit ec: ExecutionContext,
      cbf: CanBuildTo[(K, (V, W)), FS]): MapRDD[K, Tuple2[V, W], FS] = {

    val othersSilo = other.silos

    def joinSilos
      (silo1: SiloRef[S], silo2: SiloRef[S2], cbf: CanBuildTo[(K, (V, W)), FS]): SiloRef[FS] = {

      silo1.flatMap(spore {
        val lsl2 = silo2
        val lcbf1 = cbf
        sl1 => {
          lsl2.apply(spore {
            val lsl1 = sl1
            val lcbf2 = lcbf1
            sl2 => {
              lsl1.flatMap { e1 =>
                sl2.flatMap {
                  e2 => if (e1._1 == e2._1) List((e1._1, e1._2 -> e2._2)) else Nil
                }
              }(collection.breakOut(lcbf2))
            }
          })
        }
      })
    }

    def mergeSilos(receiverSilo: SiloRef[FS], restSilo: Seq[SiloRef[FS]], cbf: CanBuildTo[(K, (V, W)), FS]):
        SiloRef[FS] = {
      def innerMerge(silo1: SiloRef[FS], silo2: SiloRef[FS], cbf: CanBuildTo[(K,
        (V, W)), FS]): SiloRef[FS] = {

        silo2.flatMap(spore {
          val ls1 = silo1
          val lcbf = cbf
          s2 => {
            ls1.apply(spore {
              val ls2 = s2
              val llcbf = lcbf
              s1 => {
                val builder = llcbf()
                builder ++= s1
                builder ++= ls2
                builder.result()
              }
            })
          }
        })
      }

      restSilo.fold(receiverSilo) {
        (currentSilo, otherSilo) => innerMerge(currentSilo, otherSilo, cbf)
      }
    }

    val res = silos.map(s1 => {
      val joined = othersSilo.map(s2 =>
        // XXX: Probably inneficient: invert the argument in join, or at leat
        //the way it's joined
        joinSilos(s1, s2, cbf)
      )
      mergeSilos(joined.head, joined.tail, cbf)
    })

    new MapRDD(res, hosts)
  }

  def union(other: MapRDD[K, V, S]): MapRDD[K, V, S] = new MapRDD(silos ++ other.silos, hosts)
}
