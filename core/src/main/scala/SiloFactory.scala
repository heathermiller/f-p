package silt


trait SiloFactory[T] {
  def data: LocalSilo[T]
}
