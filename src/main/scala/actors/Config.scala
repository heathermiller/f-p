package silt
package actors

import akka.actor.ActorRef

import scala.collection.mutable
import scala.collection.concurrent.TrieMap


/**
 * Config globally accessible within the master node.
 */
object Config {

  // map ref ids to host locations
  val location: mutable.Map[Int, Host] =
    new TrieMap[Int, Host]

  // map hosts to node actor refs
  val m: mutable.Map[Host, ActorRef] =
    new TrieMap[Host, ActorRef]

}
