package silt


trait SiloFactory[U, T <: Traversable[U]] {
  def data: LocalSilo[U, T]
}
