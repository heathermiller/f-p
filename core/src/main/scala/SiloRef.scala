package silt

import scala.spores.{Spore, Spore2}
import scala.pickling.{Pickler, Unpickler}

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import MessagePicklers._


object SiloRef {

  def populate[T](host: Host, value: T)(implicit p: Pickler[InitSiloValue[T]], system: SiloSystem): SiloRef[T] = {
    val fut = system.asInstanceOf[SiloSystemInternal].initRequest[T, InitSiloValue[T]](host, { (refId: Int) =>
      InitSiloValue[T](value, refId)
    })
    Await.result(fut, 10.seconds)
  }

}

/** A program operating on data stored in a silo can only do so using a
 *  reference to the silo, a so-called `SiloRef`.
 *
 *  Similar to a proxy object, a `SiloRef` represents, and allows interacting
 *  with, a silo possibly located on a remote node. For the component acquiring
 *  a `SiloRef`, the location of the silo – local or remote – is completely
 *  transparent. We call this property *location transparency*.
 *
 *  @tparam W
 *  @tparam T the type corresponding to the referenced silo's data
 */
trait SiloRef[T] {

  /** Takes a spore that is to be applied to the data in the referenced silo.
   *
   *  Rather than immediately sending the spore across the network, and waiting
   *  for the operation to finish, the apply method is lazy: it immediately
   *  returns a SiloRef that refers to the result silo.
   *
   *  @param fun the spore to be applied on the data pointed to by this `SiloRef`
   */
  def apply[S](fun: Spore[T, S])
              (implicit pickler: Pickler[Spore[T, S]], unpickler: Unpickler[Spore[T, S]]): SiloRef[S]

  def send(): Future[T]

  def cache(): Future[SiloRef[T]]

  def flatMap[S](fun: Spore[T, SiloRef[S]])
                (implicit pickler: Pickler[Spore[T, SiloRef[S]]], unpickler: Unpickler[Spore[T, SiloRef[S]]]): SiloRef[S]

  def id: SiloRefId

  def host: Host

  def elems[W](implicit ev: T <:< Traversable[W]): ElemsSiloRef[W, T]
}

trait ElemsSiloRef[W, T] {

  def pumpTo[V, R <: Traversable[V], P <: Spore2[W, Emitter[V], Unit]](destSilo: SiloRef[R])(fun: P)
    (implicit bf: BuilderFactory[V, R], pickler: Pickler[P], unpickler: Unpickler[P]): Unit
}

final case class Host(address: String, port: Int)

final case class SiloRefId(value: Int)

// this does not extend SiloRef. Silos and SiloRefs are kept separate.
class LocalSilo[T](private[silt] val value: T) {

  def internalApply[A, B](fun: A => B): LocalSilo[B] = {
    val typedFun = fun.asInstanceOf[T => B]
    println(s"LocalSilo: value = $value")
    val res = typedFun(value)
    println(s"LocalSilo: result of applying function: $res")
    new LocalSilo[B](res)
  }

  def send(): Future[T] = {
    Future.successful(value)
  }

  def doPumpTo[A, B](existFun: Function2[A, Emitter[B], Unit], emitter: Emitter[B]): Unit = {
    val fun = existFun.asInstanceOf[Function2[Any, Emitter[B], Unit]]
    Future {
      value.asInstanceOf[Traversable[Any]].foreach { elem =>
        // println(s"visiting element $elem")
        fun(elem, emitter)
      }
      emitter.done()
    }
  }
}
