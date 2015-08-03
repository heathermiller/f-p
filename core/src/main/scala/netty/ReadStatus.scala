package silt
package netty

import scala.concurrent.Promise
import scala.collection.mutable.ArrayBuffer


/** Status of reading a message from a channel.
 */
sealed abstract class ReadStatus

// Promise `done` is completed with the entire array composed of all chunks
final case class ChunkStatus(maxSize: Int,
                             currentSize: Int,
                             chunks: ArrayBuffer[Array[Byte]],
                             done: Promise[Array[Byte]]) extends ReadStatus

final case class PartialStatus(arr: Array[Byte]) extends ReadStatus
