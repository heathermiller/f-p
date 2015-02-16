package silt
package graph

import scala.language.existentials
import scala.reflect.runtime

import scala.pickling._
import Defaults._
import shareNothing._
import binary._

import scala.concurrent.util.Unsafe


object Picklers {

  implicit def doPumpToPickler[A, B]: Pickler[DoPumpTo[A, B]] with Unpickler[DoPumpTo[A, B]] =
    new Pickler[DoPumpTo[A, B]] with Unpickler[DoPumpTo[A, B]] {
      def tag: FastTypeTag[DoPumpTo[A, B]] = implicitly[FastTypeTag[DoPumpTo[A, B]]]

      def pickle(picklee: DoPumpTo[A, B], builder: PBuilder): Unit = {
        builder.beginEntry(picklee)

        builder.putField("node", { b =>
          val node = picklee.node
          val tag = node match {
            case m: Materialized =>
              implicitly[FastTypeTag[Materialized]]
            case a: Apply[u, t, v, s] =>
              // need to pickle the erased type in this case
              FastTypeTag.mkRaw(a.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            case mi: MultiInput[r] =>
              // need to pickle the erased type in this case
              FastTypeTag.mkRaw(mi.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
          }
          b.hintTag(tag)
          NodePU.pickle(node, b)
        })

        builder.putField("fun", { b =>
          val strPickler = pickler.AllPicklers.stringPickler
          b.hintTag(strPickler.tag)
          // pickle class name of underlying function
          val underlying = picklee.fun.asInstanceOf[scala.spores.Spore2Impl[_, _, _]].f //TODO
          strPickler.pickle(underlying.getClass.getName, b)
        })
        builder.putField("emitterId", { b =>
          val intPickler = pickler.AllPicklers.intPickler
          b.hintTag(intPickler.tag)
          intPickler.pickle(picklee.emitterId, b)
        })
        builder.putField("destHost", { b =>
          val hostPickler = implicitly[Pickler[Host]]
          b.hintTag(hostPickler.tag)
          hostPickler.pickle(picklee.destHost, b)
        })
        builder.putField("destRefId", { b =>
          val intPickler = pickler.AllPicklers.intPickler
          b.hintTag(intPickler.tag)
          intPickler.pickle(picklee.destRefId, b)
        })
        builder.endEntry()
      }

      def unpickle(tag: String, reader: PReader): Any = {
        val reader1 = reader.readField("node")
        val tag1 = reader1.beginEntry()
        val node = NodePU.unpickle(tag1, reader1).asInstanceOf[Node]
        reader1.endEntry()
        val reader2 = reader.readField("fun")
        val tag2 = reader2.beginEntry()
        val funClassName = pickler.AllPicklers.stringPickler.unpickle(tag2, reader2).asInstanceOf[String]
        val fun = Unsafe.instance.allocateInstance(Class.forName(funClassName)).asInstanceOf[(A, Emitter[B]) => Unit]
        reader2.endEntry()
        val reader3 = reader.readField("emitterId")
        val tag3 = reader3.beginEntry()
        val emitterId = pickler.AllPicklers.intPickler.unpickle(tag3, reader3).asInstanceOf[Int]
        reader3.endEntry()
        val reader4 = reader.readField("destHost")
        val tag4 = reader4.beginEntry()
        val destHost = implicitly[Unpickler[Host]].unpickle(tag4, reader4).asInstanceOf[Host]
        reader4.endEntry()
        val reader5 = reader.readField("destRefId")
        val tag5 = reader5.beginEntry()
        val destRefId = pickler.AllPicklers.intPickler.unpickle(tag5, reader5).asInstanceOf[Int]
        reader5.endEntry()
        DoPumpTo[A, B](node, fun, emitterId, destHost, destRefId)
      }
    }

  implicit object GraphPU extends Pickler[Graph] with Unpickler[Graph] {
    def tag: FastTypeTag[Graph] = implicitly[FastTypeTag[Graph]]

    def pickle(picklee: Graph, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)
      builder.putField("node", { b =>
        val node = picklee.node
        val tag = node match {
          case m: Materialized =>
            implicitly[FastTypeTag[Materialized]]
          case a: Apply[u, t, v, s] =>
            // need to pickle the erased type in this case
            FastTypeTag.mkRaw(a.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
          case mi: MultiInput[r] =>
            // need to pickle the erased type in this case
            FastTypeTag.mkRaw(mi.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
        }
        b.hintTag(tag)
        NodePU.pickle(node, b)
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
      Graph(node)
    }
  }

  implicit object CommandEnvelopePU extends Pickler[CommandEnvelope] with Unpickler[CommandEnvelope] {
    def tag: FastTypeTag[CommandEnvelope] = implicitly[FastTypeTag[CommandEnvelope]]
    def pickle(picklee: CommandEnvelope, builder: PBuilder): Unit = {
      builder.beginEntry(picklee)
      builder.putField("cmd", { b =>
        val cmd = picklee.cmd
        val tag = cmd match {
          case d: DoPumpTo[a, b] =>
            // need to pickle the erased type in this case
            FastTypeTag.mkRaw(d.getClass, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
        }
        b.hintTag(tag)
        CommandPU.pickle(cmd, b)
      })
      builder.endEntry()
    }
    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("cmd")
      val tag1 = reader1.beginEntry()
      println(s"CommandEnvelopePU: unpickle, tag: $tag, tag1: $tag1")
      val cmd = CommandPU.unpickle(tag1, reader1).asInstanceOf[Command]
      reader1.endEntry()
      CommandEnvelope(cmd)
    }
  }

  implicit object CommandPU extends Pickler[Command] with Unpickler[Command] {
    def tag = implicitly[FastTypeTag[Command]]
    def pickle(picklee: Command, builder: PBuilder): Unit = picklee match {
      case d: DoPumpTo[a, b] =>
        doPumpToPickler[a, b].pickle(d, builder)
    }
    def unpickle(tag: String, reader: PReader): Any = {
      println(s"CommandPU.unpickle, tag: $tag")
      doPumpToPickler[Any, Any].unpickle(tag, reader)
    }
  }

  implicit object NodePU extends Pickler[Node] with Unpickler[Node] {
    def tag: FastTypeTag[Node] = implicitly[FastTypeTag[Node]]

    def pickle(picklee: Node, builder: PBuilder): Unit = picklee match {
      case m: Materialized =>
        implicitly[Pickler[Materialized]].pickle(m, builder)
      case a: Apply[u, t, v, s] =>
        applyPU[u, t, v, s].pickle(a, builder)
      case mi: MultiInput[r] =>
        multiInputPU[r].pickle(mi, builder)
    }

    def unpickle(tag: String, reader: PReader): Any = {
      println(s"NodePU.unpickle, tag: $tag")
      if (tag.startsWith("silt.graph.MultiInput")) {
        multiInputPU[Any].unpickle(tag, reader)
      } else if (tag.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag, reader)
      } else { // no other cases possible because of `sealed`
        applyPU[Any, Traversable[Any], Any, Traversable[Any]].unpickle(tag, reader)
      }
    }
  }

  implicit def multiInputPU[R]: Pickler[MultiInput[R]] with Unpickler[MultiInput[R]] =
    new Pickler[MultiInput[R]] with Unpickler[MultiInput[R]] {

    def tag: FastTypeTag[MultiInput[R]] = implicitly[FastTypeTag[MultiInput[R]]]

    def pickle(picklee: MultiInput[R], builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      val inputsPickler = implicitly[Pickler[::[Any]]]
      val inputsTag = implicitly[FastTypeTag[::[Any]]]
      builder.putField("inputs", { b =>
        b.hintTag(inputsTag)
        inputsPickler.pickle(picklee.inputs.asInstanceOf[::[Any]], b)
      })

      builder.putField("refId", { b =>
        b.hintTag(FastTypeTag.Int)
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
      })

      builder.putField("destHost", { b =>
        val hostPickler = implicitly[Pickler[Host]]
        b.hintTag(hostPickler.tag)
        hostPickler.pickle(picklee.destHost, b)
      })

      builder.putField("emitterId", { b =>
        b.hintTag(FastTypeTag.Int)
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.emitterId, b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val inputsUnpickler = implicitly[Unpickler[::[Any]]]
      val inputsTag = implicitly[FastTypeTag[::[Any]]]

      val reader1 = reader.readField("inputs")
      reader1.hintTag(inputsTag)
      val tag1 = reader1.beginEntry()
      val inputs = inputsUnpickler.unpickle(tag1, reader1).asInstanceOf[List[PumpNodeInput[_, _, R]]]
      reader1.endEntry()

      val reader2 = reader.readField("refId")
      reader2.hintTag(FastTypeTag.Int)
      val tag2 = reader2.beginEntry()
      val refId = pickler.AllPicklers.intPickler.unpickle(tag2, reader2).asInstanceOf[Int]
      reader2.endEntry()

      val reader3 = reader.readField("destHost")
      val hostUnpickler = implicitly[Unpickler[Host]]
      reader3.hintTag(hostUnpickler.tag)
      val tag3 = reader3.beginEntry()
      val destHost = hostUnpickler.unpickle(tag3, reader3).asInstanceOf[Host]
      reader3.endEntry()

      val reader4 = reader.readField("emitterId")
      reader4.hintTag(FastTypeTag.Int)
      val tag4 = reader4.beginEntry()
      val emitterId = pickler.AllPicklers.intPickler.unpickle(tag4, reader4).asInstanceOf[Int]
      reader4.endEntry()

      MultiInput[R](inputs, refId, destHost, emitterId)
    }
  }

  implicit def pumpNodeInputPU[U, V, R]: Pickler[PumpNodeInput[U, V, R]] with Unpickler[PumpNodeInput[U, V, R]] =
    new Pickler[PumpNodeInput[U, V, R]] with Unpickler[PumpNodeInput[U, V, R]] {

    def tag: FastTypeTag[PumpNodeInput[U,V,R]] = implicitly[FastTypeTag[PumpNodeInput[U,V,R]]]

    def pickle(picklee: PumpNodeInput[U, V, R], builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("from", { b =>
        picklee.from match {
          case m: Materialized =>
            b.hintTag(implicitly[FastTypeTag[Materialized]])
            implicitly[Pickler[Materialized]].pickle(m, b)
          case a: Apply[u, t, v, s] =>
            // need to pickle the erased type in this case
            val clazz = a.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            applyPU[u, t, v, s].pickle(a, b)
          case _ => ???
        }
      })

      builder.putField("fun", { b =>
        val clazz = picklee.fun.getClass
        println(s"class of fun: ${clazz.getName}")
        val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
        val pickler = scala.pickling.runtime.RuntimePicklerLookup.genPickler(clazz.getClassLoader, clazz, tag).asInstanceOf[Pickler[Any]]
        b.hintTag(tag)
        pickler.pickle(picklee.fun, b)
      })

      builder.putField("bf", { b =>
        val clazz = picklee.bf.getClass
        println(s"class of fun: ${clazz.getName}")
        val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
        val pickler = scala.pickling.runtime.RuntimePicklerLookup.genPickler(clazz.getClassLoader, clazz, tag).asInstanceOf[Pickler[Any]]
        b.hintTag(tag)
        pickler.pickle(picklee.bf, b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("from")
      val tag1 = reader1.beginEntry()
      val unpickler1 = scala.pickling.runtime.RuntimeUnpicklerLookup.genUnpickler(runtime.currentMirror, tag1)
      val from = unpickler1.unpickle(tag1, reader1).asInstanceOf[Node]
      reader1.endEntry()

      val reader2 = reader.readField("fun")
      val tag2 = reader2.beginEntry()
      val unpickler2 = scala.pickling.runtime.RuntimeUnpicklerLookup.genUnpickler(runtime.currentMirror, tag2)
      val fun = unpickler2.unpickle(tag2, reader2).asInstanceOf[(U, Emitter[V]) => Unit]
      reader2.endEntry()

      val reader3 = reader.readField("bf")
      val tag3 = reader3.beginEntry()
      val unpickler3 = scala.pickling.runtime.RuntimeUnpicklerLookup.genUnpickler(runtime.currentMirror, tag3)
      val bf = unpickler3.unpickle(tag3, reader3).asInstanceOf[BuilderFactory[V, R]]
      reader3.endEntry()

      PumpNodeInput[U, V, R](from, fun, bf)
    }
  }

  implicit def applyPU[U, T <: Traversable[U], V, S <: Traversable[V]]:
    Pickler[Apply[U, T, V, S]] with Unpickler[Apply[U, T, V, S]] = new Pickler[Apply[U, T, V, S]] with Unpickler[Apply[U, T, V, S]] {

    def tag: FastTypeTag[Apply[U,T,V,S]] = implicitly[FastTypeTag[Apply[U,T,V,S]]]

    def pickle(picklee: Apply[U, T, V, S], builder: PBuilder): Unit = {
      builder.beginEntry(picklee)

      builder.putField("input", { b =>
        picklee.input match {
          case m: Materialized =>
            b.hintTag(implicitly[FastTypeTag[Materialized]])
            implicitly[Pickler[Materialized]].pickle(m, b)
          case a: Apply[u, t, v, s] =>
            // need to pickle the erased type in this case
            val clazz = a.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            applyPU[u, t, v, s].pickle(a, b)
          case mi: MultiInput[r] =>
            // need to pickle the erased type in this case
            val clazz = mi.getClass
            val tag = FastTypeTag.mkRaw(clazz, runtime.currentMirror).asInstanceOf[FastTypeTag[Any]]
            b.hintTag(tag)
            multiInputPU[r].pickle(mi, b)            
        }
      })

      builder.putField("refId", { b =>
        b.hintTag(FastTypeTag.Int)
        scala.pickling.pickler.AllPicklers.intPickler.pickle(picklee.refId, b)
      })

      builder.putField("fun", { b =>
        // pickle spore
        val newBuilder = pickleFormat.createBuilder()
        newBuilder.hintTag(picklee.tag)
        picklee.pickler.asInstanceOf[Pickler[Any]].pickle(picklee.fun, newBuilder)
        val p = newBuilder.result()
        val sd = SelfDescribing(picklee.unpickler.getClass.getName, p.value)
        sd.pickleInto(b)
      })

      builder.endEntry()
    }

    def unpickle(tag: String, reader: PReader): Any = {
      val reader1 = reader.readField("input")
      val tag1 = reader1.beginEntry()
      val typeString = tag1
      println(s"applyPU typeString: $typeString")
      val input = if (typeString.startsWith("silt.graph.MultiInput")) {
        multiInputPU[Any].unpickle(tag1, reader1)
      } else if (typeString.startsWith("silt.graph.Materialized")) {
        implicitly[Unpickler[Materialized]].unpickle(tag1, reader1)
      } else { // no other cases possible because of `sealed`
        throw new Exception(s"applyPU.unpickle, typeString: $typeString")
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

      Apply[U, T, V, S](input.asInstanceOf[Node], refId.asInstanceOf[Int], fun.asInstanceOf[T => S], null, null, null)
    }
  }

}
