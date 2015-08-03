package funpass.test

import org.junit.Test

import scala.pickling._
import Defaults._
import binary._

import scala.spores._
import SporePickler._

import silt.SelfDescribing


case class Person(name: String, age: Int)


class SelfDescribingTest {

  def createSelfDescribing[T](v: T)(implicit pickler: Pickler[T], unpickler: Unpickler[T]): SelfDescribing = {
    // 1. pickle value
    val builder = pickleFormat.createBuilder()
    builder.hintTag(pickler.tag)
    pickler.pickle(v, builder)
    val p = builder.result()

    // 2. create SelfDescribing instance
    SelfDescribing(unpickler.getClass.getName, p.value)
  }

  @Test def testCaseClass(): Unit = {
    val pers = Person("joe", 40)
    val sd = createSelfDescribing(pers)

    val sdp = sd.pickle
    val binPickle = BinaryPickle(sdp.value)
    val sdu = binPickle.unpickle[SelfDescribing]
    val pers2 = sdu.result()

    assert(pers2 == pers)
  }

  @Test def testSpore(): Unit = {
    val s: Spore[Int, Int] = spore { (x: Int) => x + 1 }
    val sd = createSelfDescribing(s)

    val sdp = sd.pickle
    val binPickle = BinaryPickle(sdp.value)
    val sdu = binPickle.unpickle[SelfDescribing]
    val s2 = sdu.result().asInstanceOf[Spore[Int, Int]]
    val tmp = s2(10)

    assert(tmp == 11)
  }
}
