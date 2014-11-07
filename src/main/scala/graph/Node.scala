package silt
package graph


/** A node in the computation graph.
 *
 *  We must be able to generate a pickler for it.
 */
sealed abstract class Node {
  def refId: Int
}

final class Materialized(val refId: Int) extends Node

final class Apply[U, T <: Traversable[U], V, S <: Traversable[V]](val input: Node, val refId: Int, val fun: T => S) extends Node

final class MultiInput[R](val inputs: List[PumpNodeInput[_, _, R]], val refId: Int, val emitterId: Int) extends Node

final case class PumpNodeInput[U, V, R](from: Node, fun: (U, Emitter[V]) => Unit, bf: BuilderFactory[V, R])

// remote message
final case class Graph(node: Node)
