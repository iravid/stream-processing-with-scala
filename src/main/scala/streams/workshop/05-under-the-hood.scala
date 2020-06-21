package streams.workshop

import zio.stream._

object UnderTheHood {
  // 1. Translate the following interface to ZIO.
  trait Iter[A] {
    def next: A
  }

  // 2. Translate the following iterator transformation to ZIO.
  def zipWithIndex[A](it: Iter[A]): Iter[(A, Int)] =
    new Iter[(A, Int)] {
      var nextIdx: Int = 0
      def next: (A, Int) = {
        val idx = nextIdx
        nextIdx += 1
        (it.next, idx)
      }
    }

  // 3. Formulate a functional version of an iterator.
  class ZIterator[R, E, A] {
    def map[B](f: A => B): ZIterator[R, E, B] = ???
    def mapM: ???                             = ???
  }

  // 4. Embed chunks on our functional iterator.

  // 5. Re-implement `zipWithIndex` on ZStream.
  def zipWithIndex[R, E, A](stream: ZStream[R, E, A]): ZStream[R, E, (A, Int)] = ???

  // 6. Re-implement `ZStream#forever` recursively with `++`.
  def forever[R, E, A](stream: ZStream[R, E, A]): ZStream[R, E, A] = ???

  // 7. Check the memory usage of the implementation from exercise 6 and compare it to the
  // memory usage of the built-in ZStream#forever.
  val stream1: Stream[Nothing, Int] = forever(ZStream(1))
  val stream2: Stream[Nothing, Int] = ZStream(1).forever
}
