package silt

import scala.pickling._
import Defaults._
import binary._

import scala.collection.mutable.ListBuffer


// trait AbstractBuilder {
//   // element type
//   type Elem
//   // type of result collection
//   type Coll <: Traversable[Elem]

//   def += (elem: Elem): this.type // required, since we don't have type params!
//   def result(): Coll
//   def resultSilo(): LocalSilo[Elem, Coll]
// }

// class ListBuilder[T] extends AbstractBuilder {
//   type Elem = T
//   type Coll = List[T]

//   private val buffer = new ListBuffer[T]

//   def += (elem: T): this.type = {
//     buffer += elem
//     this
//   }

//   def result(): List[T] =
//     buffer.result()

//   def resultSilo(): LocalSilo[T, List[T]] =
//     new LocalSilo(buffer.result())
// }

// // Note: the main reason this cannot just be an Observable is that we need an
// //       implicit pickler!
// // TODO: enable signalling errors?
// trait Emitter[T] {
//   def emit(v: T)(implicit pickler: Pickler[T], unpickler: Unpickler[T]): Unit
//   def done(): Unit
// }

// trait PartitioningEmitter[T, K] extends Emitter[T] {
//   /** default partition */
//   def default(): K
//   /** emit to partition k */
//   def emitTo(k: K, v: T)(implicit p: Pickler[T]): Unit
// }
