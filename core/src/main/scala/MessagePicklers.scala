package silt

import scala.pickling._
import Defaults._
import shareNothing._
import binary._

import runtime.RuntimeUnpicklerLookup

import scala.spores._
import SporePickler._


object MessagePicklers {

  implicit def initSiloValue[T : Pickler : Unpickler]: Pickler[InitSiloValue[T]] with Unpickler[InitSiloValue[T]] = new Pickler[InitSiloValue[T]] with Unpickler[InitSiloValue[T]] {

    def tag: FastTypeTag[InitSiloValue[T]] = implicitly[FastTypeTag[InitSiloValue[T]]]

    val tp: Pickler[T] = implicitly[Pickler[T]]

    def pickle(picklee: InitSiloValue[T], builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("value", { b =>
        b.hintTag(tp.tag)
        tp.pickle(picklee.value, b)
      })

      builder.putField("refId", { b =>
        val intPickler = pickler.AllPicklers.intPickler
        b.hintTag(intPickler.tag)
        intPickler.pickle(picklee.refId, b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("value")
      val tag1 = reader1.beginEntry()
      val up = RuntimeUnpicklerLookup.genUnpickler(scala.reflect.runtime.currentMirror, tag1)
      val value = up.unpickle(tag1, reader1).asInstanceOf[T]
      reader1.endEntry()

      val reader2 = reader.readField("refId")
      val tag2 = reader2.beginEntry()
      val refId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag2, reader2).asInstanceOf[Int]
      reader2.endEntry()

      InitSiloValue(value, refId)
    }

  }

}
