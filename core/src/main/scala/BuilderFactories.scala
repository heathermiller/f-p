package silt


object BuilderFactory {

  implicit def listBuilderFactory[T]: BuilderFactory[T, List[T]] =
    new ListBuilderFactory[T]

}

trait BuilderFactory[T, R] {

  def mkBuilder(): AbstractBuilder { type Elem = T; type Coll = R }

}

// needs a pickler!
class ListBuilderFactory[T] extends BuilderFactory[T, List[T]] {

  def mkBuilder(): AbstractBuilder { type Elem = T; type Coll = List[T] } =
    new ListBuilder[T]

}
