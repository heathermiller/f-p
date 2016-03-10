package silt
package graph

import scala.language.existentials
import scala.reflect.runtime

import scala.pickling._
import Defaults._
import shareNothing._
import binary._

import scala.spores.{Spore, Spore2}

import scala.concurrent.util.Unsafe


object Picklers {

  // implicit def doPumpToPickler[A, B, P]: Pickler[DoPumpTo[A, B, P]] with Unpickler[DoPumpTo[A, B, P]] =
  //   new Pickler[DoPumpTo[A, B, P]] with Unpickler[DoPumpTo[A, B, P]] {
  //     def tag: FastTypeTag[DoPumpTo[A, B, P]] = implicitly[FastTypeTag[DoPumpTo[A, B, P]]]

  //     def pickle(picklee: DoPumpTo[A, B, P], builder: PBuilder): Unit = {
  //       builder.beginEntry(picklee)

  //       builder.putField("node", { b =>
  //         val node = picklee.node
  //         val tag = node match {
  //           case m: Materialized =>
  //             implicitly[FastTypeTag[Materialized]]
  //           case a: Apply[t, s] =>
  //             // need to pickle the erased type in this case
  //             FastTypeTag.mkRaw(a.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //           case mi: MultiInput[r] =>
  //             // need to pickle the erased type in this case
  //             FastTypeTag.mkRaw(mi.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //           case fm: FMapped[t, s] =>
  //             FastTypeTag.mkRaw(fm.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //         }
  //         b.hintTag(tag)
  //         NodePU.pickle(node, b)
  //       })

  //       builder.putField("fun", { b =>
  //         // pickle spore
  //         val newBuilder = pickleFormat.createBuilder()
  //         newBuilder.hintTag(picklee.pickler.tag)
  //         picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
  //         val p = newBuilder.result()
  //         val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
  //         sd.pickleInto(b)
  //       })

  //       builder.putField("pickler", { b =>
  //         b.hintTag(FastTypeTag.String)
  //         scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
  //       })

  //       builder.putField("unpickler", { b =>
  //         b.hintTag(FastTypeTag.String)
  //         scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
  //       })

  //       builder.putField("emitterId", { b =>
  //         val intPickler = pickler.AllPicklers.intPickler
  //         b.hintTag(intPickler.tag)
  //         intPickler.pickle(picklee.emitterId, b)
  //       })
  //       builder.putField("destHost", { b =>
  //         val hostPickler = implicitly[Pickler[Host]]
  //         b.hintTag(hostPickler.tag)
  //         hostPickler.pickle(picklee.destHost, b)
  //       })
  //       builder.putField("destRefId", { b =>
  //         val intPickler = pickler.AllPicklers.intPickler
  //         b.hintTag(intPickler.tag)
  //         intPickler.pickle(picklee.destRefId, b)
  //       })
  //       builder.endEntry()
  //     }

  //     def unpickle(tag: String, reader: PReader): Any = {
  //       val reader1 = reader.readField("node")
  //       val tag1 = reader1.beginEntry()
  //       val node = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
  //       reader1.endEntry()

  //       val reader3 = reader.readField("fun")
  //       val typestring3 = reader3.beginEntry()
  //       val unpickler3 = implicitly[Unpickler[SelfDescribing]]
  //       val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
  //       val fun = sd.result().asInstanceOf[Spore2[A, Emitter[B], Unit]]
  //       reader3.endEntry()

  //       val reader4 = reader.readField("pickler")
  //       reader4.hintTag(FastTypeTag.String)
  //       val tag4 = reader4.beginEntry()
  //       val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
  //       reader4.endEntry()
  //       val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore2[A, Emitter[B], Unit]]]

  //       val reader5 = reader.readField("unpickler")
  //       reader5.hintTag(FastTypeTag.String)
  //       val tag5 = reader5.beginEntry()
  //       val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
  //       reader5.endEntry()
  //       val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore2[A, Emitter[B], Unit]]]

  //       val reader6 = reader.readField("emitterId")
  //       val tag6 = reader6.beginEntry()
  //       val emitterId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag6, reader6).asInstanceOf[Int]
  //       reader6.endEntry()
  //       val reader7 = reader.readField("destHost")
  //       val tag7 = reader7.beginEntry()
  //       val destHost = implicitly[Unpickler[Host]].unpickle(tag7, reader7).asInstanceOf[Host]
  //       reader7.endEntry()
  //       val reader8 = reader.readField("destRefId")
  //       val tag8 = reader8.beginEntry()
  //       val destRefId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag8, reader8).asInstanceOf[Int]
  //       reader8.endEntry()

  //       DoPumpTo[A, B, Spore2[A, Emitter[B], Unit]](node, fun, pickler, unpickler, emitterId, destHost, destRefId)
  //     }
  //   }

  implicit object GraphPU extends Pickler[Graph] with Unpickler[Graph] {
    def tag: FastTypeTag[Graph] = implicitly[FastTypeTag[Graph]]

    def pickle(picklee: Graph, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)
      builder.putField("node", { b =>
        val node = picklee.node
        val tag = node match {
          case m: Materialized =>
            implicitly[FastTypeTag[Materialized]]
          case a: Apply[t, s] =>
            // need to pickle the erased type in this case
            FastTypeTag.mkRaw(a.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
          // case mi: MultiInput[r] =>
          //   // need to pickle the erased type in this case
          //   FastTypeTag.mkRaw(mi.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
          case fm: FMapped[t, s] =>
            FastTypeTag.mkRaw(fm.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
        }
        b.hintTag(tag)
        NodePU.pickle(node, b)
      })
      builder.putField("cache", { b =>
        val tag = implicitly[FastTypeTag[Boolean]]
        b.hintTag(tag)
        scala.pickling.pickler.AllPicklers.booleanPickler.pickle(picklee.cache, b)
      })
      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      // val nodeTag = implicitly[FastTypeTag[Node]]
      val reader1 = reader.readField("node")
      // reader1.hintTag(nodeTag)
      val tag1 = reader1.beginEntry()
      println(s"GraphPU: unpickle, tag: $tag, tag1: $tag1")
      val node = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
      reader1.endEntry()

      val reader2 = reader.readField("cache")
      val tag2 = reader2.beginEntry()
      val cache = scala.pickling.pickler.AllPicklers.booleanPickler.unpickle(tag2, reader2).asInstanceOf[Boolean]
      reader2.endEntry()
      Graph(node, cache)
    }
  }

  // implicit object CommandEnvelopePU extends Pickler[CommandEnvelope] with Unpickler[CommandEnvelope] {
  //   def tag: FastTypeTag[CommandEnvelope] = implicitly[FastTypeTag[CommandEnvelope]]
  //   def pickle(picklee: CommandEnvelope, builder: PBuilder): Unit = {
  //     builder.beginEntry(picklee)
  //     builder.putField("cmd", { b =>
  //       val cmd = picklee.cmd
  //       val tag = cmd match {
  //         case d: DoPumpTo[a, b, p] =>
  //           // need to pickle the erased type in this case
  //           FastTypeTag.mkRaw(d.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //       }
  //       b.hintTag(tag)
  //       CommandPU.pickle(cmd, b)
  //     })
  //     builder.endEntry()
  //   }
  //   def unpickle(tag: String, reader: PReader): Any = {
  //     val reader1 = reader.readField("cmd")
  //     val tag1 = reader1.beginEntry()
  //     println(s"CommandEnvelopePU: unpickle, tag: $tag, tag1: $tag1")
  //     val cmd = CommandPU.unpickle(tag1, reader1).asInstanceOf[Command]
  //     reader1.endEntry()
  //     CommandEnvelope(cmd)
  //   }
  // }
   
  // implicit object CommandPU extends Pickler[Command] with Unpickler[Command] {
  //   def tag = implicitly[FastTypeTag[Command]]
  //   def pickle(picklee: Command, builder: PBuilder): Unit = picklee match {
  //     case d: DoPumpTo[a, b, p] =>
  //       doPumpToPickler[a, b, p].pickle(d, builder)
  //   }
  //   def unpickle(tag: String, reader: PReader): Any = {
  //     println(s"CommandPU.unpickle, tag: $tag")
  //     doPumpToPickler[Any, Any, Any].unpickle(tag, reader)
  //   }
  // }

  implicit object NodePU extends Pickler[Node] with Unpickler[Node] {
    def tag: FastTypeTag[Node] = implicitly[FastTypeTag[Node]]

    def pickle(picklee: Node, builder: PBuilder): Unit = picklee match {
      case m: Materialized =>
        implicitly[Pickler[Materialized]].pickle(m, builder)
      case a: Apply[t, s] =>
        applyPU[t, s].pickle(a, builder)
      case fm: FMapped[t, s] =>
        fmappedPU[t, s].pickle(fm, builder)
    }

    def unpickle(tag: String, reader: PReader): Any = {
      println(s"NodePU.unpickle, tag: $tag")
      if (tag.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag, reader)
      } else if (tag.startsWith("silt.graph.FMapped")) {
        fmappedPU[Any, Any].unpickle(tag, reader)
      } else if (tag.startsWith("silt.graph.Apply")) {
        applyPU[Any, Any].unpickle(tag, reader)
      } else {
        throw new Exception(s"Trying to unpickle non-existing node with type string: ${tag}")
      }
    }
  }

  // implicit def multiInputPU[R]: Pickler[MultiInput[R]] with Unpickler[MultiInput[R]] =
  //   new Pickler[MultiInput[R]] with Unpickler[MultiInput[R]] {

  //   def tag: FastTypeTag[MultiInput[R]] = implicitly[FastTypeTag[MultiInput[R]]]

  //   def pickle(picklee: MultiInput[R], builder: PBuilder): Unit = {
  //     builder.beginEntry(picklee)

  //     val inputsPickler = scala.pickling.pickler.AllPicklers.seqPickler[PumpNodeInput[Any, Any, R, Any]]
  //     val inputsTag = implicitly[FastTypeTag[Seq[PumpNodeInput[Any, Any, R, Any]]]]

  //     builder.putField("inputs", { b =>
  //       b.hintTag(inputsTag)
  //       inputsPickler.pickle(picklee.inputs.asInstanceOf[Seq[PumpNodeInput[Any, Any, R, Any]]], b)
  //     })

  //     builder.putField("refId", { b =>
  //       b.hintTag(FastTypeTag.Int)
  //       scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
  //     })

  //     builder.putField("destHost", { b =>
  //       val hostPickler = implicitly[Pickler[Host]]
  //       b.hintTag(hostPickler.tag)
  //       hostPickler.pickle(picklee.destHost, b)
  //     })

  //     builder.putField("emitterId", { b =>
  //       b.hintTag(FastTypeTag.Int)
  //       scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.emitterId, b)
  //     })

  //     builder.endEntry()
  //   }

  //   def unpickle(tag: String, reader: PReader): Any = {
  //     // println(s"multiInputPU.unpickle, tag: $tag")
  //     val inputsUnpickler = scala.pickling.pickler.AllPicklers.seqPickler[PumpNodeInput[Any, Any, R, Any]]
  //     val inputsTag = implicitly[FastTypeTag[Seq[PumpNodeInput[Any, Any, R, Any]]]]

  //     val reader1 = reader.readField("inputs")
  //     reader1.hintTag(inputsTag)
  //     val tag1 = reader1.beginEntry()
  //     val inputs = inputsUnpickler.unpickle(tag1, reader1).asInstanceOf[Seq[PumpNodeInput[_, _, R, _]]]
  //     reader1.endEntry()

  //     val reader2 = reader.readField("refId")
  //     reader2.hintTag(FastTypeTag.Int)
  //     val tag2 = reader2.beginEntry()
  //     val refId = pickler.AllPicklers.intPickler.unpickle(tag2, reader2).asInstanceOf[Int]
  //     reader2.endEntry()

  //     val reader3 = reader.readField("destHost")
  //     val hostUnpickler = implicitly[Unpickler[Host]]
  //     reader3.hintTag(hostUnpickler.tag)
  //     val tag3 = reader3.beginEntry()
  //     val destHost = hostUnpickler.unpickle(tag3, reader3).asInstanceOf[Host]
  //     reader3.endEntry()

  //     val reader4 = reader.readField("emitterId")
  //     reader4.hintTag(FastTypeTag.Int)
  //     val tag4 = reader4.beginEntry()
  //     val emitterId = pickler.AllPicklers.intPickler.unpickle(tag4, reader4).asInstanceOf[Int]
  //     reader4.endEntry()

  //     MultiInput[R](inputs, refId, destHost, emitterId)
  //   }
  // }

  // implicit def pumpNodeInputPU[U, V, R, P]: Pickler[PumpNodeInput[U, V, R, P]] with Unpickler[PumpNodeInput[U, V, R, P]] =
  //   new Pickler[PumpNodeInput[U, V, R, P]] with Unpickler[PumpNodeInput[U, V, R, P]] {

  //   def tag: FastTypeTag[PumpNodeInput[U,V,R,P]] = implicitly[FastTypeTag[PumpNodeInput[U,V,R,P]]]

  //   def pickle(picklee: PumpNodeInput[U, V, R, P], builder: PBuilder): Unit = {
  //     builder.beginEntry(picklee)

  //     builder.putField("from", { b =>
  //       picklee.from match {
  //         case m: Materialized =>
  //           b.hintTag(implicitly[FastTypeTag[Materialized]])
  //           implicitly[Pickler[Materialized]].pickle(m, b)
  //         case a: Apply[t, s] =>
  //           // need to pickle the erased type in this case
  //           val clazz = a.getClass
  //           val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //           b.hintTag(tag)
  //           applyPU[t, s].pickle(a, b)
  //         case fm: FMapped[t, s] =>
  //           val clazz = fm.getClass
  //           val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //           b.hintTag(tag)
  //           fmappedPU[t, s].pickle(fm, b)
  //         case _ => ???
  //       }
  //     })

  //     builder.putField("fromHost", { b =>
  //       val hostPickler = implicitly[Pickler[Host]]
  //       b.hintTag(hostPickler.tag)
  //       hostPickler.pickle(picklee.fromHost, b)
  //     })

  //     builder.putField("fun", { b =>
  //       // pickle spore
  //       val newBuilder = pickleFormat.createBuilder()
  //       newBuilder.hintTag(picklee.pickler.tag)
  //       picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
  //       val p = newBuilder.result()
  //       val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
  //       sd.pickleInto(b)
  //     })

  //     builder.putField("pickler", { b =>
  //       b.hintTag(FastTypeTag.String)
  //       scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
  //     })

  //     builder.putField("unpickler", { b =>
  //       b.hintTag(FastTypeTag.String)
  //       scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
  //     })

  //     builder.putField("bf", { b =>
  //       val clazz = picklee.bf.getClass
  //       println(s"class of fun: ${clazz.getName}")
  //       val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
  //       val pickler = scala.pickling.runtime.RuntimePicklerLookup.genPickler(clazz.getClassLoader, clazz, tag).asInstanceOf[Pickler[Any]]
  //       b.hintTag(tag)
  //       pickler.pickle(picklee.bf, b)
  //     })

  //     builder.endEntry()
  //   }

  //   def unpickle(tag: String, reader: PReader): Any = {
  //     val reader1 = reader.readField("from")
  //     val tag1 = reader1.beginEntry()
  //     // println(s"pumpNodeInputPU: unpickle, tag: $tag, tag1: $tag1")
  //     val from = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
  //     reader1.endEntry()

  //     val reader2 = reader.readField("fromHost")
  //     val hostUnpickler = implicitly[Unpickler[Host]]
  //     reader2.hintTag(hostUnpickler.tag)
  //     val tag2 = reader2.beginEntry()
  //     val fromHost = hostUnpickler.unpickle(tag2, reader2).asInstanceOf[Host]
  //     reader2.endEntry()

  //     val reader3 = reader.readField("fun")
  //     val typestring3 = reader3.beginEntry()
  //     val unpickler3 = implicitly[Unpickler[SelfDescribing]]
  //     val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
  //     val fun = sd.result().asInstanceOf[Spore2[U, Emitter[V], Unit]]
  //     reader3.endEntry()

  //     val reader4 = reader.readField("pickler")
  //     reader4.hintTag(FastTypeTag.String)
  //     val tag4 = reader4.beginEntry()
  //     val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
  //     reader4.endEntry()
  //     val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore2[U, Emitter[V], Unit]]]

  //     val reader5 = reader.readField("unpickler")
  //     reader5.hintTag(FastTypeTag.String)
  //     val tag5 = reader5.beginEntry()
  //     val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
  //     reader5.endEntry()
  //     val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore2[U, Emitter[V], Unit]]]

  //     val reader6 = reader.readField("bf")
  //     val tag6 = reader6.beginEntry()
  //     val unpickler6 = scala.pickling.runtime.RuntimeUnpicklerLookup.genUnpickler(runtime.currentMirror, tag6)
  //     val bf = unpickler6.unpickle(tag6, reader6).asInstanceOf[BuilderFactory[V, R]]
  //     reader6.endEntry()

  //     PumpNodeInput[U, V, R, Spore2[U, Emitter[V], Unit]](from, fromHost, fun, pickler, unpickler, bf)
  //   }
  // }

  implicit def applyPU[T, S]:
    Pickler[Apply[T, S]] with Unpickler[Apply[T, S]] = new Pickler[Apply[T, S]] with Unpickler[Apply[T, S]] {

    def tag: FastTypeTag[Apply[T,S]] = implicitly[FastTypeTag[Apply[T,S]]]

    def pickle(picklee: Apply[T, S], builder: PBuilder): Unit = {
      // println(s"applyPU: pickling $picklee")
      builder.beginEntry(picklee)

      builder.putField("input", { b =>
        picklee.input match {
          case m: Materialized =>
            b.hintTag(implicitly[FastTypeTag[Materialized]])
            implicitly[Pickler[Materialized]].pickle(m, b)
          case a: Apply[t, s] =>
            // need to pickle the erased type in this case
            val clazz = a.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            applyPU[t, s].pickle(a, b)
          // case mi: MultiInput[r] =>
          //   // need to pickle the erased type in this case
          //   val clazz = mi.getClass
          //   val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
          //   b.hintTag(tag)
          //   multiInputPU[r].pickle(mi, b)
          case fm: FMapped[t, s] =>
            val clazz = fm.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            fmappedPU[t, s].pickle(fm, b)
        }
      })

      builder.putField("refId", { b =>
        b.hintTag(FastTypeTag.Int)
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
      })

      builder.putField("fun", { b =>
        // pickle spore
        val newBuilder = pickleFormat.createBuilder()
        newBuilder.hintTag(picklee.pickler.tag)
        picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
        val p = newBuilder.result()
        val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
        sd.pickleInto(b)
      })

      builder.putField("pickler", { b =>
        b.hintTag(FastTypeTag.String)
        scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
      })

      builder.putField("unpickler", { b =>
        b.hintTag(FastTypeTag.String)
        scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("input")
      val tag1 = reader1.beginEntry()
      val typeString = tag1
      println(s"applyPU typeString: $typeString")
      val input = if (typeString.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag1, reader1)
      } else if (typeString.startsWith("silt.graph.FMapped")) {
        fmappedPU[Any, Any].unpickle(tag1, reader1)
      } else if (typeString.startsWith("silt.graph.Apply")) {
        applyPU[Any, Any].unpickle(tag1, reader1)
      } else {
        throw new Exception(s"Trying to unpickle non-existing node with type string: ${typeString}")
      }
      reader1.endEntry()

      val reader2 = reader.readField("refId")
      reader2.hintTag(FastTypeTag.Int)
      val tag2 = reader2.beginEntry()
      val refId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag2, reader2)
      reader2.endEntry()

      val reader3 = reader.readField("fun")
      val typestring3 = reader3.beginEntry()
      val unpickler3 = implicitly[Unpickler[SelfDescribing]]
      val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
      val fun = sd.result()
      reader3.endEntry()

      val reader4 = reader.readField("pickler")
      reader4.hintTag(FastTypeTag.String)
      val tag4 = reader4.beginEntry()
      val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
      reader4.endEntry()
      val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore[T, S]]]

      val reader5 = reader.readField("unpickler")
      reader5.hintTag(FastTypeTag.String)
      val tag5 = reader5.beginEntry()
      val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
      reader5.endEntry()
      val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore[T, S]]]

      Apply[T, S](input.asInstanceOf[Node], refId.asInstanceOf[Int], fun.asInstanceOf[Spore[T, S]], pickler, unpickler)
    }
  }

  implicit def fmappedPU[T, S]:
      Pickler[FMapped[T, S]] with Unpickler[FMapped[T, S]] = new Pickler[FMapped[T, S]] with Unpickler[FMapped[T, S]] {

    def tag: FastTypeTag[FMapped[T, S]] = implicitly[FastTypeTag[FMapped[T, S]]]

    def pickle(picklee: FMapped[T, S], builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("input", { b =>
        picklee.input match {
          case m: Materialized =>
            b.hintTag(implicitly[FastTypeTag[Materialized]])
            implicitly[Pickler[Materialized]].pickle(m, b)
          case a: Apply[t, s] =>
            // need to pickle the erased type in this case
            val clazz = a.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            applyPU[t, s].pickle(a, b)
          case fm: FMapped[t, s] =>
            val clazz = fm.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            fmappedPU[t, s].pickle(fm, b)
        }

        builder.putField("refId", { b =>
          b.hintTag(FastTypeTag.Int)
          scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
        })

        builder.putField("fun", { b =>
          val newBuilder = pickleFormat.createBuilder()
          newBuilder.hintTag(picklee.pickler.tag)
          picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
          val p = newBuilder.result()
          val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
          sd.pickleInto(b)
        })

        builder.putField("pickler", { b =>
          b.hintTag(FastTypeTag.String)
          scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.pickler.getClass.getName, b)
        })

        builder.putField("unpickler", { b =>
          b.hintTag(FastTypeTag.String)
          scala.pickling.pickler.AllPicklers.stringPickler.pickle(picklee.unpickler.getClass.getName, b)
        })
      })
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("input")
      val tag1 = reader.beginEntry()
      val typeString = tag1
      val input = if (typeString.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag1, reader1)
      } else if (typeString.startsWith("silt.graph.FMapped")) {
        fmappedPU[Any, Any].unpickle(tag1, reader1)
      } else if (typeString.startsWith("silt.graph.Apply")) {
        applyPU[Any, Any].unpickle(tag1, reader1)
      } else {
        throw new Exception(s"Trying to unpickle non-existing node with type string: ${typeString}")
      }
      reader1.endEntry()

      val reader2 = reader.readField("refId")
      reader2.hintTag(FastTypeTag.Int)
      val tag2 = reader2.beginEntry()
      val refId = scala.pickling.pickler.AllPicklers.intPickler.unpickle(tag2, reader2)
      reader2.endEntry()

      val reader3 = reader.readField("fun")
      val typestring3 = reader3.beginEntry()
      val unpickler3 = implicitly[Unpickler[SelfDescribing]]
      val sd = unpickler3.unpickle(typestring3, reader3).asInstanceOf[SelfDescribing]
      val fun = sd.result()
      reader3.endEntry()

      val reader4 = reader.readField("pickler")
      reader4.hintTag(FastTypeTag.String)
      val tag4 = reader4.beginEntry()
      val picklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag4, reader4).asInstanceOf[String]
      reader4.endEntry()
      val pickler = Unsafe.instance.allocateInstance(Class.forName(picklerClassName)).asInstanceOf[Pickler[Spore[T, SiloRef[S]]]]

      val reader5 = reader.readField("unpickler")
      reader5.hintTag(FastTypeTag.String)
      val tag5 = reader5.beginEntry()
      val unpicklerClassName = scala.pickling.pickler.AllPicklers.stringPickler.unpickle(tag5, reader5).asInstanceOf[String]
      reader5.endEntry()
      val unpickler = Unsafe.instance.allocateInstance(Class.forName(unpicklerClassName)).asInstanceOf[Unpickler[Spore[T, SiloRef[S]]]]

      FMapped[T, S](input.asInstanceOf[Node], refId.asInstanceOf[Int], fun.asInstanceOf[Spore[T, SiloRef[S]]], pickler, unpickler)
    }
  }

}
