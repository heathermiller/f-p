package silt
package netty

import scala.language.existentials

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.util.ReferenceCountUtil

import scala.pickling._
import shareNothing._
import Defaults._
import binary._

import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import java.io.ByteArrayOutputStream
import java.util.concurrent.BlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import silt.graph._
import Picklers._


final class ReceptorRunnable(queue: BlockingQueue[HandleIncoming], system: SystemImpl, host: Host) extends Runnable with SendUtils {
  @volatile var shouldTerminate = false

  def systemImpl = system

  // maps SiloRef refIds to promises of local silo instances
  private val promiseOf: mutable.Map[Int, Promise[LocalSilo[_, _]]] = new TrieMap[Int, Promise[LocalSilo[_, _]]]

  //TODO: do we need to use TrieMap here?
  private val builderOfEmitterId: mutable.Map[Int, (AbstractBuilder, Int, Int)] = new TrieMap[Int, (AbstractBuilder, Int, Int)]

  val numPickled = new AtomicInteger(0)

  private def getOrElseInitPromise(id: Int): Promise[LocalSilo[_, _]] = promiseOf.get(id) match {
    case None =>
      println("no promise found")
      val newPromise = Promise[LocalSilo[_, _]]()
      promiseOf += (id -> newPromise)
      newPromise
    case Some(promise) =>
      println("found promise")
      promise
  }

  // PERF: instantiation is expensive, since it awaits a future for the destination channel
  private class RemoteEmitter[T](destHost: Host, emitterId: Int, destRefId: Int) extends Emitter[T] {
    val destChannel = Await.result(system.talkTo(destHost), 5.seconds)

    def emit(v: T)(implicit pickler: Pickler[T], unpickler: Unpickler[T]): Unit = {
      // println(s"EMITTER [to $destRefId]: EMIT")

      try {
        // 1. pickle value
        val builder = pickleFormat.createBuilder()
        builder.hintTag(pickler.tag)
        pickler.pickle(v, builder)
        val p = builder.result()
        numPickled.incrementAndGet()

        // 2. create SelfDescribing instance
        val sd = SelfDescribing(unpickler.getClass.getName, p.value)

        // 3. pickle SelfDescribing instance
        val sdp = sd.pickle
        val ba = sdp.value
        val msg = Emit(emitterId, destRefId, ba)
        // println(s"EMITTER [to $destRefId]: sending $msg")
        system.sendToChannel(destChannel, msg)
      } catch {
        case t: Throwable =>
          t.printStackTrace()
      }
    }

    def done(): Unit = {
      val msg = Done(emitterId, destRefId)
      system.sendToChannel(destChannel, msg)
    }
  }

  /** Handle an incoming local message.
   *
   *  @param command        the received message (already unpickled)
   *  @param ctx            the channel handler context
   *  @param resultPromise  a empty promise that is completed with an optional reply message
   */
  private def handleIncomingLocal(command: Any, ctx: ChannelHandlerContext, resultPromise: Promise[Option[Any]]): Unit =
    command match { // each case must complete `resultPromise`
      case Terminate() =>
        println(s"SERVER: closing ${ctx.channel()}")
        ctx.close().sync()

        for ((_, status) <- system.statusOf) status match {
          case Connected(ch, group) =>
            println(s"SERVER: closing $ch")
            ch.close().sync()
            group.shutdownGracefully()
          case _ =>
            /* do nothing */
        }

        system.latch.countDown()
        resultPromise.success(None)

      case theMsg @ InitSiloFun(fun, refId) =>
        println(s"SERVER: creating silo using class $fun...")

        system.location += (refId -> host)
        val promise = getOrElseInitPromise(refId)
        Future {
          val newSilo = fun()
          promise.success(newSilo)
          println(s"SERVER: created $newSilo (${newSilo.value}). responding...")
          val replyMsg = OKCreated(refId)
          replyMsg.id = theMsg.id
          resultPromise.success(Some(replyMsg))
        }

      case theMsg @ InitSilo(fqcn, refId) =>
        println(s"SERVER: creating Silo using class $fqcn...")

        system.location += (refId -> host)
        val promise = getOrElseInitPromise(refId)

        val clazz = Class.forName(fqcn)
        println(s"SERVER: looked up $clazz")
        val inst = clazz.newInstance()
        println(s"SERVER: created instance $inst")

        inst match {
          case factory: SiloFactory[u, t] =>
            val silo = factory.data // COMPUTE-INTENSIVE
            promise.success(silo)

            system.localSiloRefOf += (refId -> silo)

            println(s"SERVER: created $silo. responding...")

            val replyMsg = OKCreated(refId)
            replyMsg.id = theMsg.id
            resultPromise.success(Some(replyMsg))

          case _ => /* do nothing */
            resultPromise.success(None)
        }

      // TODO: remove if unused
      case theMsg: ApplyMessage[u, a, v, b] =>
        val name    = theMsg.refId
        val fun     = theMsg.fun
        val newName = theMsg.newRefId

        print(s"SERVER: sending function to DS '$name': ")
        // look up DS
        val theDS = system.localSiloRefOf(name)//.asInstanceOf[DS[Any]]
        println(theDS.toString)

        // val oldFun: T => S = fun
        // val newFun: Any => Any = (arg: Any) => {
        //   val typedArg = arg.asInstanceOf[T]
        //   println(s"newFun: typedArg = $typedArg")
        //   println(s"newFun: fun = $fun")
        //   oldFun.apply(typedArg)
        // }

        // newDS is guaranteed to be local
        val newDS = theDS.internalApply[a, v, b](fun)
        println(s"SERVER: value of new DS: ${newDS.value}")
        system.localSiloRefOf += (newName -> newDS)
        resultPromise.success(None)

      // TODO: remove if unused
      case theMsg @ ForceMessage(name) =>
        println(s"SERVER: forcing SiloRef '$name'...")
        // look up SiloRef
        val theDS = system.localSiloRefOf(name)//.asInstanceOf[SiloRef[Any]]

        val replyMsg = ForceResponse(theDS/*.asInstanceOf[LocalSilo[Any]]*/.value)
        replyMsg.id = theMsg.id
        resultPromise.success(Some(replyMsg))

      case msg @ Graph(n) =>
        println(s"SERVER: received graph with node $n")
        n match { // in each case complete `resultPromise` with ForceResponse(value)
          case m: Materialized =>
            promiseOf(m.refId).future.foreach { (silo: LocalSilo[_, _]) =>
              val replyMsg = ForceResponse(silo.value)
              replyMsg.id = msg.id
              resultPromise.success(Some(replyMsg))
            }

          case app: Apply[u, t, v, s] =>
            val fun = app.fun
            val promise = getOrElseInitPromise(app.refId)

            if (promise.isCompleted) {
              val res = promise.future.value.get
              resultPromise.success(Some(ForceResponse(res)))
            } else {
              val inputPromise = Promise[Option[Any]]()
              // println("handling Apply, app.input: " + app.input)
              val localMsg = new HandleIncomingLocal(Graph(app.input), ctx, inputPromise)
              queue.add(localMsg)
              inputPromise.future.foreach { case Some(ForceResponse(value)) =>
                // println(s"yay: input graph is materialized")
                val res = fun(value.asInstanceOf[t])
                val newSilo = new LocalSilo[v, s](res)
                promise.success(newSilo)
                resultPromise.success(Some(ForceResponse(res)))
              }
            }

          case m: MultiInput[r] =>
            val inputs    = m.inputs
            val refId     = m.refId
            val destHost  = m.destHost
            val emitterId = m.emitterId

            val promise = getOrElseInitPromise(refId)

            // prepare builder
            inputs match {
              case List()  =>
                promise.failure(new NoSuchElementException("no input silo"))
              case x :: xs =>
                println(s"NODE $refId: creating builder")
                val builder = x.bf.mkBuilder()
                // add mapping for emitterId
                // builder, num completed, required completed
                val triple = (builder, 0, inputs.size)
                builderOfEmitterId += (emitterId -> triple)
                // send DoPumpTo messages to inputs
                inputs.foreach { case input: PumpNodeInput[u, v, r, p] =>
                  print("looking up src host...")
                  val srcHost = input.fromHost
                  println(s"$srcHost.")
                  system.talkTo(srcHost).map { channel =>
                    // must also send node (input.from), so that the input silo can be completed first
                    println(s"NODE $refId: sending DoPumpTo to ${input.from.refId}")
                    val msg = CommandEnvelope(DoPumpTo(input.from, input.fun, input.pickler, input.unpickler, emitterId, destHost, refId))
                    system.sendToChannel(channel, msg)
                  }
                }
                // register completion for responding with ForceResponse
                promise.future.foreach { (silo: LocalSilo[_, _]) =>
                  resultPromise.success(Some(ForceResponse(silo.value)))
                }
            }

          case _ =>
            throw new Exception("boom")
        }

      case CommandEnvelope(pump: DoPumpTo[a, b, p]) =>
        val node      = pump.node
        val fun       = pump.fun
        val emitterId = pump.emitterId
        val destHost  = pump.destHost // currently unused, but might be useful for Netty backend
        val destRefId = pump.destRefId

        println(s"NODE ${node.refId}: received DoPumpTo")

        val emitter = new RemoteEmitter[b](destHost, emitterId, destRefId)

        print(s"NODE ${node.refId}: getOrElseInitPromise... ")
        val promise = getOrElseInitPromise(node.refId)
        promise.future.foreach { localSilo =>
          // println(s"SERVER: calling doPumpTo on $localSilo (${localSilo.value})")
          localSilo.doPumpTo[a, b](fun.asInstanceOf[(a, silt.Emitter[b]) => Unit], emitter)
        }

        // kick off materialization
        println(s"NODE ${node.refId}: kick off materialization by sending Graph($node)")
        // self ! Graph(node)
        val localMsg = new HandleIncomingLocal(Graph(node), ctx, Promise[Option[Any]]())
        queue.add(localMsg)

      case Emit(emitterId, destRefId, ba) =>
        builderOfEmitterId.get(emitterId) match {
          case None => ???
          case Some((builder, current, required)) =>
            val pickle = BinaryPickleArray(ba.asInstanceOf[Array[Byte]])
            val sdv = pickle.unpickle[SelfDescribing] // *static* unpickling
            val v = sdv.result()
            // println(s"received ${v.toString}")
            val stableBuilder = builder
            stableBuilder += v.asInstanceOf[stableBuilder.Elem]
        }

      case Done(emitterId, destRefId) =>
        builderOfEmitterId.get(emitterId) match {
          case None => ???
          case Some((builder, current, required)) =>
            if (current == required - 1) {
              // we're done, last emitter finished

              // here we need to call result() on the builder
              // but it would be better to directly create a local Silo
              val newSilo = builder.resultSilo()

              // complete promise for destRefId, so that force calls can complete
              promiseOf(destRefId).success(newSilo)

              // println(s"SERVER: created silo containing: ${newSilo.value}")
            } else {
              val newTriple = (builder, current + 1, required)
              builderOfEmitterId += (emitterId -> newTriple)
            }
        }

    }

  def unpickleAndHandle(arr: Array[Byte], ctx: ChannelHandlerContext): Unit = {
    // TODO: unpickle asynchronously
    val pickle = BinaryPickle(arr)
    // println(s"SERVER: unpickling incoming byte array")
    val command = try pickle.unpickle[Any] catch {
      case t: Throwable =>
        println(s"exception while attempting unpickle with runtime pickler:")
        t.printStackTrace()
    }
    // println(s"SERVER: received $command")

    val resultPromise = Promise[Option[Any]]()
    handleIncomingLocal(command, ctx, resultPromise)
    resultPromise.future.foreach {
      case Some(replyMsg) => sendToChannel(ctx.channel(), replyMsg)
      case None => /* do nothing */
    }
  }

  var chunkStatus: Option[ReadStatus] = None

  /** Handle incoming message received via given channel.
   *
   *  @param msg  required to be a Netty `ByteBuf`
   *  @param ctx  the context
   */
  private def handleIncoming(msg: Any, ctx: ChannelHandlerContext): Unit = {
    val in: ByteBuf = msg.asInstanceOf[ByteBuf]
    val bos = new ByteArrayOutputStream
    try {
      while (in.isReadable()) bos.write(in.readByte().asInstanceOf[Int])
    } finally {
      ReferenceCountUtil.release(msg)
    }

    var chunk = bos.toByteArray()
    var finished = false
    while (!finished) {
      chunkStatus match {
        case None => // have received first chunk
          // read length (first 4 bytes)
          var maxSize: Int = 0
          maxSize |= (chunk(0) << 24)
          maxSize |= (chunk(1) << 16) & 0xFF0000
          maxSize |= (chunk(2) << 8 ) & 0xFF00
          maxSize |= (chunk(3)      ) & 0xFF
          // chunk complete?
          if (chunk.length == maxSize + 4) {
            // println(s"CHUNK: read chunk has EXACT size [$maxSize bytes]")
            finished = true
            unpickleAndHandle(chunk.drop(4), ctx)
          } else if (chunk.length > maxSize + 4) {
            val regularChunk = chunk.slice(4, maxSize + 4) //TODO: PERF
            unpickleAndHandle(regularChunk, ctx)
            // new chunk to process next in the loop
            chunk = chunk.drop(maxSize + 4)
          } else {
            // println(s"CHUNK: read chunk has SMALLER size [read: ${chunk.length - 4} bytes, max: $maxSize bytes]")
            finished = true
            val done = Promise[Array[Byte]]()
            done.future.foreach(ba => unpickleAndHandle(ba, ctx))
            chunkStatus = Some(ReadStatus(maxSize, chunk.length - 4, ArrayBuffer(chunk.drop(4)), done))
          }

        case Some(status @ ReadStatus(maxSize, size, chunks, done)) =>
          val newSize = size + chunk.length
          if (newSize < maxSize) {
            chunks += chunk
            chunkStatus = Some(status.copy(currentSize = newSize))
            finished = true
          } else if (newSize == maxSize) {
            chunks += chunk
            chunkStatus = None
            val arr = chunks.flatten.toArray
            done.success(arr)
            finished = true
          } else if (newSize > maxSize) {
            // chunk.length is too much
            // use only maxSize - size elements from the current chunk
            val useOnly = maxSize - size
            val regularChunk = chunk.take(useOnly) //TODO: PERF
            // new chunk to process next in the loop
            chunk = chunk.drop(useOnly)
            chunks += regularChunk
            chunkStatus = None
            val arr = chunks.flatten.toArray
            done.success(arr)
          }
      }
    }
  }

  def run(): Unit = {
    while (!shouldTerminate) {
      // Wait for next message.
      try {
        queue.take() match {
          case l: HandleIncomingLocal =>
            handleIncomingLocal(l.msg, l.ctx, l.resultPromise)
          case HandleIncoming(msg, ctx) =>
            handleIncoming(msg, ctx)
          case _ =>
            // TODO: unexpected object in queue.
        }
      } catch {
        case ie: InterruptedException =>
          // continue to check `shouldTerminate`
      }
    }
  }
}
