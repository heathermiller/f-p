package silt
package graph

import scala.spores._
import scala.pickling._
import Defaults._


/** A node in the computation graph.
 *
 *  We must be able to generate a pickler for it.
 */
sealed abstract class Node {
  def refId: Int
}

final case class Materialized(refId: Int) extends Node

final case class Apply[U, T <: Traversable[U], V, S <: Traversable[V]]
                      (input: Node, refId: Int, fun: T => S, tag: FastTypeTag[Spore[T, S]], pickler: Pickler[Spore[T, S]], unpickler: Unpickler[Spore[T, S]]) extends Node

final case class MultiInput[R](inputs: List[PumpNodeInput[_, _, R]], refId: Int, emitterId: Int) extends Node

final case class PumpNodeInput[U, V, R](from: Node, fun: (U, Emitter[V]) => Unit, bf: BuilderFactory[V, R])

// remote message
final case class Graph(node: Node) extends ReplyMessage
