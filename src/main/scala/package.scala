import scala.language.higherKinds

import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelFutureListener

package object silt {

  type Transformer[A, B, C[_] <: Traversable[_]] = (C[A], A => B) => C[B] // stateless

  type Combiner[A, B, C[_] <: Traversable[_]] = (C[A], B, (B, A) => B) => B // stateless

  type Accessor[A, B, C[_] <: Traversable[_]] = (C[A], A => B) => B // stateless

}
