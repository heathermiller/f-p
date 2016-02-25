import scala.language.higherKinds

import scala.collection.generic._

case class Bar[T, Container[A] <: Traversable[A]](content: Container[T]) {

  def foo[A](f: T => A)(implicit bf: CanBuildFrom[Container[T], A, Container[A]]): Container[A] = {
    foo[A, Container](f)
  }

  def foo[A, C[A]](f: T => A)(implicit bf: CanBuildFrom[C[T], A, C[A]]): C[A] = {
    val builder = bf()
    content.foreach { elem => builder += f(elem) }
    builder.result
  }

  // def flatMap[A](f: T => Container[A])(implicit bf: CanBuildFrom[Container[T], Container[A], Container[Container[A]]]): Container[A] = {
  //   val builder = bf()
  //   content.foreach { elem => builder += f(elem) }
  //   builder.result.flatten
  // }
}

object Randomer {
  def main(args: Array[String]): Unit = {
    val l = List(1, 2, 3)
    val foo = new Bar(l)
    println(foo.foo[Int, Array](_ + 1))
  }
}
