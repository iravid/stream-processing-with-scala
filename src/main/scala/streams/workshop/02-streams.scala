package streams.workshop

import zio._
import zio.clock.Clock
import zio.stream._
import java.nio.file.Path
import java.io.IOException

object StreamTypes {
  // 1. A stream that emits integers and cannot fail.

  // 2. A stream that emits strings and can fail with throwables.

  // 3. A stream that emits no elements.

  // 4. A stream that requires access to the console, can fail with
  // string errors and emits integers.
}

object ConstructingStreams {
  // One of the main design goals of ZStream is extremely good interoperability
  // for various scenarios. A consequence of that is a wide range of constructors
  // for streams. In this section, we will survey various ways to construct
  // streams.

  // 1. Construct a stream with a single integer, '42'.
  val single: ??? = ???

  // 2. Construct a stream with three characters, 'a', 'b', 'c'.
  val chars: ??? = ???

  // 3. Construct a stream from an effect that reads a line from the user.
  val readLine: ??? = ???

  // 4. Construct a stream that fails with the string "boom".
  val failed: ??? = ???

  // 5. Create a stream that extracts the Clock from the environment.
  val clockStream: ZStream[???, ???, Clock] = ???

  // 6. Construct a stream from an existing list of numbers:
  val ns: List[Int] = List.fill(100)(1)
  val nStream: ???  = ???

  // 7. Using repeatEffectOption, repeatedly read lines from the user
  // until the string "EOF" is entered.
  val allLines: ??? = ???

  // 8. Drain an iterator using `repeatEffectOption`.
  def drainIterator[A](iterator: Iterator[A]): ZStream[???, ???, A] =
    ???

  // 9. Using ZStream.unwrap, unwrap the stream embedded in this effect.
  val wrapped        = ZIO(ZStream(1, 2, 3))
  val unwrapped: ??? = ???

  // 10. Using ZStream.unfold, create a stream that emits the numbers 1 to 10.
  val oneTo10: ??? = ???

  // 11. Do the same with unfoldM, but now sleep for `n` milliseconds after
  // emitting every number `n`.
  val oneTo10WithSleeps: ??? = ???

  // 12. Read an array in chunks using unfoldChunkM.
  def readArray[A](array: Array[A], chunkSize: Int): ZStream[???, ???, A] =
    ???

  // 13. Read an array in chunks using paginateM. You'll need to use
  // `flattenChunks` in this exercise.
  def readArray2[A](array: Array[A], chunkSize: Int): ZStream[???, ???, A] =
    ???

  // 14. Implement `tail -f`-like functionality using ZStream.
  def tail(path: Path, chunkSize: Int): ZStream[Clock, Throwable, Byte] =
    ???
}

object TransformingStreams {
  // In this section, we will cover operators for synchronous transformations
  // on streams. These are the bread and butter of stream operators, so we'll
  // use them quite a bit as we create stream processing programs.

  // 1. Transform a stream of ints to a stream of strings.
  val warmup: ??? = ZStream(1, 2, 3) ?

  // 2. Multiply every integer of the stream using a coefficient
  // retrieved effectfully.
  val currentCoefficient: ZIO[random.Random, Nothing, Double] =
    random.nextDoubleBetween(0.5, 0.85)
  val multiplied: ??? = ZStream.range(1, 10) ?

  // 3. Split every string to separate lines in this stream.
  val lines: ZStream[Any, Nothing, String] =
    ZStream("line1\nline2", "line3\n\nline4\n")

  // 4. Print out a JSON array from the following stream of strings
  // using intersperse and tap.
  val data = ZStream("ZIO", "ZStream", "ZSink")

  // 5. Read a 100 even numbers from the Random generator.
  val hundredEvens: ZStream[random.Random, Nothing, Int] = ???

  // 6. Read 10 lines from the user, but skip the first 3.
  val linesDropped: ZStream[console.Console, IOException, String] = ???

  // 7. Read 10 lines from the user, but drop all lines until the user
  // writes the word "START". Don't include "START" in the stream.
  val finalEx: ZStream[console.Console, IOException, String] = ???

  // 7. Using ZStream#++, print a message to the user, then read a line.
  val printAndRead: ZStream[console.Console, IOException, String] = ???

  // 8. Split the following stream of CSV data to individual tokens
  // that are emitted on a 2-second schedule
  val scheduled: ZStream[???, Nothing, String] =
    ZStream("DDOG,12.7,12.8", "NET,10.1,10.2") ?

  // 9. Terminate this infinite stream as soon as a `Left` is emitted.
  val terminateOnLeft: ZStream[random.Random, Nothing, ???] =
    ZStream.repeatEffect(random.nextBoolean.map(if (_) Left(()) else Right(()))) ?

  // 10. Do the same but with `Option` and `None`:
  val terminateOnNone: ZStream[random.Random, Nothing, ???] =
    ZStream.repeatEffect(random.nextBoolean.map(if (_) Some(()) else None)) ?
}
