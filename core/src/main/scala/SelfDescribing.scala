package silt

import scala.pickling._
import Defaults._
import binary._

import scala.reflect.runtime.currentMirror

final case class SelfDescribing(unpicklerClassName: String, blob: Array[Byte]) {
  def result(): Any = {
    val pickle = BinaryPickleArray(blob)
    val reader = pickleFormat.createReader(pickle)

    val unpicklerInst = try {
      Class.forName(unpicklerClassName).newInstance().asInstanceOf[Unpickler[Any]]
    } catch {
      case _: Throwable =>
        scala.concurrent.util.Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Any]]
    }

    val typeString = reader.beginEntry()
    reader.hintTag(unpicklerInst.tag)
    unpicklerInst.unpickle(unpicklerInst.tag.key, reader)
  }
}

object SelfDescribingTest extends App {

  def test(): Unit = {
    val pair: (Int, List[String]) = (4, List("a", "b"))

    // 1. generate unpickler
    val unpickler = implicitly[Unpickler[(Int, List[String])]]
    // 2. pickle value
    val p = pair.pickle
    // println(p.value)

    // 3. create SelfDescribing instance
    val sd = SelfDescribing(unpickler.getClass.getName, p.value)

    // 4. pickle SelfDescribing instance
    val sdp = sd.pickle

    // 5. unpickle SelfDescribing instance
    val up = sdp.unpickle[SelfDescribing]

    // 6. call result() to unpickle the wrapped blob
    val res = up.result()
    println(res)
  }

  test()
}
