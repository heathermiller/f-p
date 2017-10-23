package funpass.test

import org.junit.Test

import scala.pickling._
import Defaults._
import binary._

import scala.spores._
import SporePickler._

import silt._
import MessagePicklers._
import silt.graph._
import silt.graph.Picklers._

class MessagePicklingTest {
  def createSelfDescribing[T](v: T)(implicit pickler: Pickler[T], unpickler: Unpickler[T]): SelfDescribing = {
    // 1. pickle value
    val builder = pickleFormat.createBuilder()
    builder.hintTag(pickler.tag)
    pickler.pickle(v, builder)
    val p = builder.result()

    // 2. create SelfDescribing instance
    SelfDescribing(unpickler.getClass.getName, p.value)
  }

  @Test
  def testPickleInitSiloValue(): Unit = {
    val l = List(11, 12, 13)
    val msg = InitSiloValue(l, 1)

    val sd = createSelfDescribing(msg)

    val sdp = sd.pickle
    val binPickle = BinaryPickle(sdp.value)
    val sdu = binPickle.unpickle[SelfDescribing]
    val msg2 = sdu.result()
    msg2 match {
      case InitSiloValue(v, id) =>
        assert(v == List(11, 12, 13))
    }
  }
}
