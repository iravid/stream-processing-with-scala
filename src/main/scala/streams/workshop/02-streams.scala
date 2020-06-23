package streams.workshop

import zio._
import zio.duration._
import zio.clock.Clock
import zio.stream._
import java.nio.file.Path
import java.io.IOException

object StreamTypes {

  // val st: ZStream[console.Console, Nothing, Int] = ???
  // val st2: ZStream[Any, Nothing, Nothing] = ???
  // val z: ZIO[Any, Nothing, Nothing] = ???

  // 1. A stream that emits integers and cannot fail.
  type IntStream = ZStream[Any, Nothing, Int]

  // 2. A stream that emits strings and can fail with throwables.
  type StringThrowableStream = ZStream[Any, Throwable, String]

  // 3. A stream that emits no elements and could fail with integers.
  type NoElements = ZStream[Any, Int, Nothing]

  // 4. A stream that requires access to the console, can fail with
  // string errors and emits integers.
  type Final = ZStream[console.Console, String, Int]

  // type Stream[A] = ZIO[Any, Nothing, (A, Stream[A])]
}

object ConstructingStreams {
  // One of the main design goals of ZStream is extremely good interoperability
  // for various scenarios. A consequence of that is a wide range of constructors
  // for streams. In this section, we will survey various ways to construct
  // streams.

  val st = ZStream(1, 2, 3)

  val s: ZStream[random.Random, Nothing, Int] = ZStream.fromEffect(random.nextInt)

  // 1. Construct a stream with a single integer, '42'.
  val single: ZStream[Any, Nothing, Int] = ZStream(42)

  // 2. Construct a stream with three characters, 'a', 'b', 'c'.
  val chars: ZStream[Any, Nothing, Char] = ZStream('a', 'b', 'c')

  // 3. Construct a stream from an effect that reads a line from the user.
  val readLine: ZStream[console.Console, IOException, String] = ZStream.fromEffect(console.getStrLn)

  // 4. Construct a stream that fails with the string "boom".
  val failed: ZStream[Any, String, Nothing] = ZStream.fail("Boom")

  // 5. Create a stream that extracts the Clock from the environment.
  val zio: ZIO[Clock, Nothing, Clock]             = ZIO.environment[Clock]
  val clockStream: ZStream[Clock, Nothing, Clock] = ZStream.environment[Clock]

  // 6. Construct a stream from an existing list of numbers:
  val ns: List[Int]                       = List.fill(100)(1)
  val nStream: ZStream[Any, Nothing, Int] = ZStream.fromIterable(ns)

  // 7. Using repeatEffectOption, repeatedly read lines from the user
  // until the string "EOF" is entered.
  val allLines = ZStream.repeatEffectOption(
    console.getStrLn.foldM(e => ZIO.fail(Some(e)), {
      case "EOF"     => ZIO.fail(None)
      case otherwise => ZIO.succeed(otherwise)
    })
  )

  // 8. Drain an iterator using `repeatEffectOption`.
  def drainIterator[A](it: Iterator[A]): ZStream[Any, Throwable, A] =
    ZStream.repeatEffectOption {
      ZIO(it.hasNext).mapError(Some(_)).flatMap { hasNext =>
        if (hasNext) ZIO(it.next).mapError(Some(_))
        else ZIO.fail(None)
      }
    }

  val effectProducingStream = console.getStrLn.flatMap { line =>
    Task(line.toInt).map(elements => ZStream.fromIterable(List.fill(elements)(5)))
  }

  // 9. Using ZStream.unwrap, unwrap the stream embedded in this effect.
  val wrapped   = ZIO(ZStream(1, 2, 3))
  val unwrapped = ZStream.unwrap(wrapped)

  // 10. Using ZStream.unfold, create a stream that emits the numbers 1 to 10.
  val oneTo10: Stream[Nothing, Int] =
    ZStream.unfold(1) { i =>
      if (i <= 10) Some((i, i + 1))
      else None
    }

  // 11. Do the same with unfoldM, but now sleep for `n` milliseconds after
  // emitting every number `n`.
  val oneTo10WithSleeps = ZStream.unfoldM(1) { i =>
    // body
    ZIO.sleep((i - 1).millis) *>
      (if (i <= 10) ZIO.succeed(Some((i, i + 1)))
       else ZIO.succeed(None))
  }

  // 12. Read an array in chunks using unfoldChunkM.
  def readArray[A](array: Array[A], chunkSize: Int): ZStream[Any, Nothing, A] =
    ZStream.unfoldChunkM(0) { currIdx =>
      UIO {
        if (currIdx >= array.size) None
        else {
          val chunk = Chunk.fromArray(array.slice(currIdx, math.min(currIdx + chunkSize, array.size)))
          Some((chunk, currIdx + chunkSize))
        }
      }
    }

  // 13. Read an array in chunks using paginateM. You'll need to use
  // `flattenChunks` in this exercise.
  def readArray2[A](array: Array[A], chunkSize: Int): ZStream[Any, Nothing, A] =
    ZStream
      .paginateM(0) { currIdx =>
        UIO {
          Chunk.fromArray(array.slice(currIdx, math.min(currIdx + chunkSize, array.size))) ->
            (if ((currIdx + chunkSize) >= array.size) None else Some(currIdx + chunkSize))
        }
      }
      .flattenChunks

  // 14. Implement `tail -f`-like functionality using ZStream.
  def tail(path: Path, chunkSize: Int): ZStream[Clock, Throwable, Byte] =
    ???
}

object TransformingStreams {
  // In this section, we will cover operators for synchronous transformations
  // on streams. These are the bread and butter of stream operators, so we'll
  // use them quite a bit as we create stream processing programs.

  // 1. Transform a stream of ints to a stream of strings.
  val warmup: ZStream[Any, Nothing, String] = ZStream(1, 2, 3).map(_.toString)

  // 2. Multiply every integer of the stream using a coefficient
  // retrieved effectfully.
  val currentCoefficient: ZIO[random.Random, Nothing, Double] =
    random.nextDoubleBetween(0.5, 0.85)
  val multiplied: ZStream[console.Console with random.Random, Nothing, Double] =
    ZStream
      .range(1, 10)
      .mapM(i => currentCoefficient.map(_ * i))
      .mapM(i => console.putStrLn(i.toString).as(i))

  // 3. Split every string to separate lines in this stream.
  val lines: ZStream[Any, Nothing, String] =
    ZStream("line1\nline2", "line3\n\nline4\n")
      .mapConcat(s => s.split('\n'))

  // 4. Print out a JSON array from the following stream of strings
  // using intersperse and tap.
  val data = ZStream("ZIO", "ZStream", "ZSink")
    .map(el => s""""${el}"""")
    .intersperse("[", ", ", "]")
    .tap(console.putStrLn(_))

  // 5. Read a 100 even numbers from the Random generator.
  val hundredEvens: ZStream[random.Random, Nothing, Int] =
    ZStream.repeatEffect(random.nextInt).filter(_ % 2 == 0).take(100)

  // 6. Read 10 lines from the user, but skip the first 3.
  val linesDropped: ZStream[console.Console, IOException, String] =
    ZStream.repeatEffect(console.getStrLn).take(10).drop(3)

  // ZStream.repeatEffectWith(console.getStrLn, Schedule.recurs(10))
  // ZStream.repeatEffect(console.getStrLn).take(10)

  // 7. Read 10 lines from the user, but drop all lines until the user
  // writes the word "START". Don't include "START" in the stream.
  val finalEx: ZStream[console.Console, IOException, String] =
    ZStream.repeatEffect(console.getStrLn).dropUntil(_ == "START").take(10)

  // 8. Using ZStream#++, print a message to the user, then read a line.
  val printAndRead: ZStream[console.Console, IOException, String] =
    ZStream.fromEffect(console.putStrLn("enter your name")).drain ++
      ZStream.fromEffect(console.getStrLn)

  // 9. Split the following stream of CSV data to individual tokens
  // that are emitted on a 2-second schedule, using ZStream#flatMap
  // and ZStream#schedule.
  val scheduled: ZStream[???, Nothing, String] =
    ZStream("DDOG,12.7,12.8", "NET,10.1,10.2")
      .flatMap { line =>
        ZStream
          .fromIterable(line.split(","))
          .schedule(Schedule.fixed(2.seconds))
      }
      .tap(console.putStrLn(_))

  // 11. Terminate this infinite stream as soon as a `Left` is emitted.
  val terminateOnLeft: ZStream[random.Random, Nothing, Unit] =
    ZStream.repeatEffect(random.nextBoolean.map(if (_) Left(()) else Right(())))
      .collectWhile {
        case Right(value) => value
      }

  // 12. Do the same but with `Option` and `None`:
  val terminateOnNone: ZStream[random.Random, Nothing, Unit] =
    ZStream.repeatEffect(random.nextBoolean.map(if (_) Some(()) else None))
      .collectWhileSome
}
