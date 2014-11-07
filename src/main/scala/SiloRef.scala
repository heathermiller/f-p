package silt

import scala.spores._

import scala.pickling._
import binary._

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.collection.Traversable


/*
object SiloRef {
  def apply[T](v: T): SiloRef[T] = {
    new LocalSilo(v)
  }
}
*/

trait SiloRef[W, T <: Traversable[W]] {
  def apply[V, S <: Traversable[V]](fun: Spore[T, S]): SiloRef[V, S]

  def send(): Future[T]

  def pumpTo[V, R <: Traversable[V]](destSilo: SiloRef[V, R])(fun: Spore2[W, Emitter[V], Unit])
                                    (implicit bf: BuilderFactory[V, R], pickler: SPickler[V], unpickler: Unpickler[V]): Unit = ???

  def id: SiloRefId
}

final case class Host(host: String, port: Int)

final case class SiloRefId(value: Int)

// this does not extend SiloRef. Silos and SiloRefs are kept separate.
class LocalSilo[U, T <: Traversable[U]](private[silt] val value: T) {
/*
  def apply[S](fun: T => S): SiloRef[S] = {
    println(s"LocalSiloRef: value = $value")
    val res = fun(value)
    println(s"LocalSiloRef: result of applying function: $res")
    new LocalSilo(res)
  }
*/

  def internalApply[A, V, B <: Traversable[V]](fun: A => B): LocalSilo[V, B] = {
    val typedFun = fun.asInstanceOf[T => B]
    println(s"LocalSilo: value = $value")
    val res = typedFun(value)
    println(s"LocalSilo: result of applying function: $res")
    new LocalSilo[V, B](res)
  }

  def send(): Future[T] = {
    Future.successful(value)
  }

  def doPumpTo[A, B](existFun: Function2[A, Emitter[B], Unit], emitter: Emitter[B]): Unit = {
    val fun = existFun.asInstanceOf[Function2[U, Emitter[B], Unit]]
    Future {
      value.foreach { elem =>
        // println(s"visiting element $elem")
        fun(elem, emitter)
      }
      emitter.done()
    }
  }
}
