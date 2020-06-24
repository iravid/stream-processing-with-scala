package streams.workshop

import zio._
import zio.duration._
import zio.stream._
import java.nio.file.Path
import zio.duration.Duration

object ControlFlow {
  // 1. Write a stream that reads bytes from one file,
  // then prints "Done reading 1", then reads another bytes
  // from another file.
  def bytesFromHereAndThere(here: Path, there: Path, bytesAmount: Int): ZStream[???, ???, Byte] =
    ???

  // 2. What would be the difference in output between these two streams?
  val output1 =
    ZStream("Hello", "World").tap(console.putStrLn(_)).drain ++
      ZStream.fromEffect(console.putStrLn("Done printing!")).drain

  val output2 =
    ZStream("Hello", "World").tap(console.putStrLn(_)) *>
      ZStream.fromEffect(console.putStrLn("Done printing!"))

  // 3. Read 4 paths from the user. For every path input, read the file in its entirety
  // and print it out. Once done printing it out, log the file name in a `Ref` along with
  // its character count. Once the stream is completed, print out the all the files and
  // counts.
  val read4Paths: ZStream[???, ???, ???] = ???
}

object StreamErrorHandling {
  // 1. Modify this stream, which is under our control, to survive transient exceptions.
  trait TransientException { self: Exception => }
  def query: RIO[random.Random, Int] = random.nextIntBetween(0, 11).flatMap { n =>
    if (n < 2) Task.fail(new RuntimeException("Unrecoverable"))
    else if (n < 6) Task.fail(new RuntimeException("recoverable") with TransientException)
    else Task.succeed(n)
  }

  val queryResults: ZStream[random.Random with clock.Clock, Throwable, Int] =
    ZStream.repeatEffect(query.retry(Schedule.fixed(50.millis)))

  // 2. Apply retries to the transformation applied during this stream.
  type Lookup = Has[Lookup.Service]
  object Lookup {
    trait Service {
      def lookup(i: Int): Task[String]
    }

    def lookup(i: Int): RIO[Lookup, String] = ZIO.accessM[Lookup](_.get.lookup(i))

    def live: ZLayer[Any, Nothing, Lookup] =
      ZLayer.succeed { i =>
        if (i < 5) Task.fail(new RuntimeException("Lookup failure"))
        else Task.succeed("Lookup result")
      }
  }

  val queryResultsTransformed: ZStream[random.Random with Lookup, Throwable, Int] =
    ZStream.repeatEffect(query).mapM(i => Lookup.lookup(i).map((i, _))) ?

  // 3. Switch to another stream once the source fails in this stream.
  val failover: ZStream[Any, Nothing, Int] =
    ZStream(ZIO.succeed(1), ZIO.fail("Boom")).mapM(identity)
      .orElse(ZStream.succeed(2))

  // 4. Do the same, but when the source fails, print out the failure and switch
  // to the stream specified in the failure.
  case class UpstreamFailure[R, E, A](reason: String, backup: ZStream[R, E, A])
  val failover2: ZStream[console.Console, UpstreamFailure[Any, Nothing, Int], Int] =
    ZStream(ZIO.succeed(1), ZIO.fail(UpstreamFailure("Malfunction", ZStream(2))))
      .mapM(identity)
      .catchAll { case UpstreamFailure(reason, backup) =>
        ZStream.fromEffect(console.putStrLn(reason)).drain ++ backup
      }

  // 5. Implement a simple retry combinator that waits for the specified duration
  // between attempts.
  def retryStream[R, E, A](stream: ZStream[R, E, A], interval: Duration): ZStream[???, ???, ???] = ???

  // 6. Measure the memory usage of this stream:
  val alwaysFailing = retryStream(ZStream.fail("Boom"), 1.millis)

  // 7. Surface typed errors as value-level errors in this stream using `either`:
  val eithers = ZStream(ZIO.succeed(1), ZIO.fail("Boom"), ZIO.succeed(3)).mapM(identity).either

  // 8. Use catchAll to restart this stream, without re-acquiring the resource.
  val subsection = ZStream
    .bracket(console.putStrLn("Acquiring"))(_ => console.putStrLn("Releasing"))
    .flatMap(_ => ZStream(ZIO.succeed(1), ZIO.fail("Boom")))
}

object Concurrency {
  // 1. Create a stream that prints every element from a queue.
  val queuePrinter: ZStream[???, ???, ???] = ZStream.fromQueue(???) ?

  // 2. Run the queuePrinter stream in one fiber, and feed it elements
  // from another fiber. The latter fiber should run a stream that reads
  // lines from the user.
  val queuePipeline: ??? = ???

  // 3. Introduce the ability to signal end-of-stream on the queue pipeline.
  val queuePipelineWithEOS: ??? = ???

  // 4. Introduce the ability to signal errors on the queue pipeline.
  val queuePipelineWithEOSAndErrors: ??? = ???

  // 5. Combine the line reading stream with the line printing stream using
  // ZStream#drainFork.
  val backgroundDraining: ??? = ???

  // 6. Prove to yourself that drainFork terminates the background stream
  // when the foreground ends by:
  //   a. creating a stream that uses `ZStream.bracketExit` to print on interruption and
  //      delaying it with ZStream.never
  //   b. draining it in the background of another stream with drainFork
  //   c. making the foreground stream end after a 1 second delay
  val drainForkTerminates: ??? = ???

  // 7. Simplify our queue pipeline with `ZStream#buffer`.
  val buffered: ??? = ???

  // 8. Open a socket server, and wait and read from incoming connections for
  // 30 seconds.
  val timedSocketServer: ??? = ???

  // 9. Create a stream that emits once after 30 seconds. Apply interruptAfter(10.seconds)
  // and haltAfter(10.seconds) to it and note the difference in behavior.
  val interruptVsHalt: ??? = ???

  // 10. Use timeoutTo to avoid the delay embedded in this stream and replace the last
  // element with "<TIMEOUT>"
  val csvData = (ZStream("symbol,price", "AAPL,500") ++
    ZStream.fromEffect(clock.sleep(30.seconds)).drain ++
    ZStream("AAPL,501")) ?

  // 11. Generate 3 random prices with a 5 second delay between each price
  // for every symbol in the following stream.
  val symbols = ZStream("AAPL", "DDOG", "NET") ?

  // 12. Regulate the output of this infinite stream of records to emit records
  // on exponentially rising intervals, from 50 millis up to a maximum of 5 seconds.
  def pollSymbolQuote(symbol: String): URIO[random.Random, (String, Double)] =
    random.nextDoubleBetween(0.1, 0.5).map((symbol, _))
  val regulated = ZStream.repeatEffect(pollSymbolQuote("V")) ?

  // 13. Introduce a parallel db writing operator in this stream. Handle up to
  // 5 records in parallel.
  case class Record(value: String)
  def writeRecord(record: Record): RIO[clock.Clock with console.Console, Unit] =
    console.putStrLn(s"Writing ${record}") *> clock.sleep(1.second)

  val dbWriter = ZStream.repeatEffect(random.nextString(5).map(Record(_))) ?

  // 14. Test what happens when one of the parallel operations encounters an error.
  val whatHappens =
    ZStream(
      clock.sleep(5.seconds).onExit(ex => console.putStrLn(s"First element exit: ${ex.toString}")),
      clock.sleep(5.seconds).onExit(ex => console.putStrLn(s"Second element exit: ${ex.toString}")),
      clock.sleep(1.second) *> ZIO.fail("Boom")
    ).mapMPar(3)(identity)
}

object StreamComposition {
  // 1. Merge these two streams.
  val one    = ZStream.repeatWith("left", Schedule.fixed(1.second).jittered)
  val two    = ZStream.repeatWith("right", Schedule.fixed(500.millis).jittered)
  val merged = ???

  // 2. Merge `one` and `two` above with the third stream here:
  val three       = ZStream.repeatWith("middle", Schedule.fixed(750.millis).jittered)
  val threeMerges = ???

  // 3. Emulate drainFork with mergeTerminateLeft.
  val emulation: ??? =
    ZStream
      .bracket(Queue.bounded[Long](16))(_.shutdown)
      .flatMap { queue =>
        val writer = ZStream.repeatEffectWith(clock.nanoTime >>= queue.offer, Schedule.fixed(1.second))
        val reader = ZStream.fromQueue(queue).tap(l => console.putStrLn(l.toString))

        val (_, _) = (writer, reader)

        ZStream.unit
    } ?

  // 4. Sum the numbers between the two streams, padding the right one with zeros.
  val left  = ZStream.range(1, 8)
  val right = ZStream.range(1, 5)

  val zipped = ???

  // 5. Regulate the output of this stream by zipping it with another stream that ticks on a fixed schedule.
  val highVolume = ZStream.range(1, 100).forever ?

  // 6. Truncate this stream by zipping it with a `Take` stream that is fed from a queue.
  val toTruncate = ZStream.range(1, 100).forever ?

  // 7. Perform a deterministic merge of these two streams in a 1-2-2-1 pattern.
  val cats = ZStream("bengal", "shorthair", "chartreux").forever
  val dogs = ZStream("labrador", "poodle", "boxer").forever

  // 8. Write a stream that starts reading and emitting DDOG.csv every time the user
  // prints enter.
  val echoingFiles = ???

  // 9. Run a variable number of background streams (according to the parameter) that
  // perform monitoring. They should interrupt the foreground fiber doing the processing.
  def doMonitor(id: Int) =
    ZStream.repeatEffectWith(
      random.nextIntBetween(1, 11).flatMap { i =>
        if (i < 8) console.putStrLn(s"Monitor OK from ${id}")
        else Task.fail(new RuntimeException(s"Monitor ${id} failed!"))
      },
      Schedule.fixed(2.seconds).jittered
    )

  val doProcessing =
    ZStream.repeatEffectWith(
      random.nextDoubleBetween(0.1, 0.5).map(f => s"Sample: ${f}"),
      Schedule.fixed(1.second).jittered
    )

  def runProcessing(nMonitors: Int) = {
    val _ = nMonitors
    doProcessing ?
  }

  // 10. Write a stream that starts reading and emitting DDOG.csv every time the user
  // prints enter, but only keeps the latest 5 files open.
  val echoingFilesLatest = ???
}
