package silt
package netty

import scala.spores.Spore

import scala.pickling._
import binary._

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class RemoteSiloRef[U, T <: Traversable[U]](val id: SiloRefId, val system: SystemImpl) extends SiloRef[U, T] with SendUtils {

  def systemImpl: SystemImpl = system

  // for the returned DS[S], create a new, unique name *locally*
  // (to support pickling of RemoteDS[T]'s, make sure when it is unpickled, it gets a new unique name on the local node.)
  // these are not intended to be pickled, though.
  def apply[V, S <: Traversable[V]](fun: Spore[T, S]): SiloRef[V, S] = {
    val port = Config.m(id.value)
    val host = Host("127.0.0.1", port)
    println(s"RemoteSiloRef: connecting to $host...")
    val futChannel = system.talkTo(host)

    val newRefId = system.refIds.incrementAndGet()
    // new DS ends up on the same node as `this`
    Config.m += (newRefId -> port)

    val sent = futChannel.andThen { case tr =>
      val msg = ApplyMessage[U, T, V, S](id.value, fun, newRefId)
      sendToChannel(tr.get, msg)
    }

    Await.ready(sent, 2.seconds)

    new RemoteSiloRef[V, S](SiloRefId(newRefId), system)
  }

  def send(): Future[T] = {
    val port = Config.m(id.value)
    val host = Host("127.0.0.1", port)
    println(s"RemoteDS: connecting to $host...")
    val futChannel = system.talkTo(host)

    val intermediate = Promise[Boolean]()

    val response = futChannel.flatMap { ch =>
      val msg = ForceMessage(id.value)
      msg.id = system.seqNum.incrementAndGet()
      val resFut = sendWithReply(ch, msg)
      intermediate.success(true)
      resFut
    }.map { case ForceResponse(value) =>
      value.asInstanceOf[T]
    }

    Await.ready(intermediate.future, 2.seconds)
    response
  }

}
