package siltx

import akka.actor.{ActorRef, Actor, ActorSystem, Props}

import scala.pickling._
import binary._

import scala.collection.Traversable

import silt.{AbstractBuilder, ListBuilder, Emitter, PartitioningEmitter}


// IDEA: send values in pre-serialized form: this enables use of static picklers,
//       since we don't have to pickle a value of type `Any`!
//       (we pickle each value at the point where we still have the type info!)
class ActorEmitter[T, R](ref: ActorRef, builder: AbstractBuilder { type Elem = T; type Coll = R }) extends Emitter[T] {
  // first, send builder
  ref ! builder

  def emit(v: T)(implicit pickler: SPickler[T], tag: FastTypeTag[T], unpickler: Unpickler[T]): Unit = {
    // has to be sent to actor in pre-serialized form
    val ba = v//.pickle.value
    ref ! ba
  }

  def done(): Unit = {
    ref ! "Done"
  }
}

class DummyReceiver extends Actor {
  var builder: AbstractBuilder = _ // type Elem and type Coll left abstract

  def receive = {
    case b: AbstractBuilder =>
      builder = b

    case a: Array[Byte] =>
      val pickle = BinaryPickleArray(a)
      val v = pickle.unpickle[Any] // runtime unpickling
      println(s"received ${v.toString}")
      val stableBuilder = builder
      stableBuilder += v.toString.asInstanceOf[stableBuilder.Elem]

    case "Done" =>
      println(s"result of builder: ${builder.result().toString}")
  }
}

class SiloRef[T](v: T) {

  /**
   *  Only applicable if T is a subtype of Traversable[U].
   */
  def fold[S, U, V](initial: S, emit: Emitter[V])(fun: (S, U, Emitter[V]) => S, end: (S, Emitter[V]) => Unit = (acc: S, emit: Emitter[V]) => {})
                   (implicit travT: T <:< Traversable[U]): Unit = {
    val trav = travT(v) // `v` as a Traversable[U]
    var accum = initial
    trav.foreach { (elem: U) =>
      accum = fun(accum, elem, emit)
    }
    end(accum, emit)
    // signal end to `emit`
    emit.done()
  }

  def pump[U, V](emit: Emitter[V], fun: (U, Emitter[V]) => Unit)(implicit travT: T <:< Traversable[U]): Unit = {
    val trav = travT(v) // `v` as a Traversable[U]
    ???
  }
}

object Scratch extends App {
  val sys = ActorSystem("test-system")

  val silo = new SiloRef(List(1, 2, 3))
  val recv = sys.actorOf(Props[DummyReceiver])

  val emitter = new ActorEmitter[String, List[String]](recv, new ListBuilder[String])

  silo.fold(0, emitter) { (acc: Int, elem: Int, emit: Emitter[String]) =>
    emit.emit(s"[acc:$acc,elem:$elem]")
    acc + elem
  }

  val recv2 = sys.actorOf(Props[DummyReceiver])

  // reduction, 1 silo
  val emitter2 = new ActorEmitter[String, List[String]](recv2, new ListBuilder[String])

  silo.fold(0, emitter2)(
    (acc: Int, elem: Int, emit: Emitter[String]) => acc + elem,
    (acc: Int, emit: Emitter[String]) => emit.emit(s"result: $acc")
  )

  Thread.sleep(1000)
  sys.shutdown()
}
