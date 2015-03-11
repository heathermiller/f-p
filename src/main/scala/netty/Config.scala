package silt
package netty

import scala.collection.mutable
import scala.collection.concurrent.TrieMap


// for now, every server has hard-coded config
object Config {

  // map node ids to port nums
  val m: mutable.Map[Int, Int] = {
    val trie = new TrieMap[Int, Int]
    trie += (0 -> 8090)
    trie += (1 -> 8091)
    trie += (2 -> 8092)
    trie += (3 -> 8093)
    trie
  }

}
