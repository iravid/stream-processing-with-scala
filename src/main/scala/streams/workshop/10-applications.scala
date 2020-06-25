package streams.workshop

import zio._
import zio.duration._
import zio.stream._

object Applications {
  // 1. Write a layer that represents a database client that supports
  // streaming results out of the database. Be sure to create an accessor
  // for streaming SQL results out using ZStream.accessStream.
  // Pattern: using a stream produced by a module
  class DatabaseClient(data: Ref[Map[String, String]]) {

    def readData: ZStream[Any, Nothing, (String, String)] = ZStream.fromIterableM(data.get)
    def close: UIO[Unit]                                  = UIO.unit
  }
  object DatabaseClient {
    def make = Ref.make(Map[String, String]()).map(new DatabaseClient(_)).toManaged_
  }

  // 2. Run these 3 streams in the main entrypoint of an application such that they
  // will run in the background, and any error that occurs in them will cause the
  // application to shut down.
  // Pattern: direct composition of streams into the application
  val kafkaConsumerStream =
    ZStream.bracket(console.putStrLn("Opening Kafka consumer"))(_ => console.putStrLn("Closing Kafka consumer")) *>
      ZStream.repeatEffectWith(
        random.nextIntBetween(0, 11).flatMap { i =>
          if (i < 7) console.putStrLn("Processing record ...")
          else Task.fail(new RuntimeException("Kafka failure!"))
        },
        Schedule.fixed(2.seconds).jittered
      )
  val incomingTcpDataStream =
    ZStream.bracket(console.putStrLn("Opening TCP data stream"))(_ => console.putStrLn("Closing TCP data stream")) *>
      ZStream.repeatEffectWith(
        random.nextIntBetween(0, 11).flatMap { i =>
          if (i < 7) console.putStrLn("Connection received, streaming to S3")
          else Task.fail(new RuntimeException("TCP failure!"))
        },
        Schedule.fixed(10.seconds).jittered
      )
  val httpServer = ZStream
    .bracket(console.putStrLn("Listening on port 8080"))(_ => console.putStrLn("Unbinding HTTP server"))
    .mapM(_ => ZIO.never)

  val app: ZIO[ZEnv, Nothing, ExitCode] = ???

  // 3. Create a layer that sets up a background cache invalidation fiber.
  // Pattern: streams as background processes
  class Cache(ref: Ref[Map[String, String]]) {
    def get(key: String): UIO[Option[String]]      = ref.get.map(_.get(key))
    def set(key: String, value: String): UIO[Unit] = ref.update(_.updated(key, value))
    def empty: UIO[Unit]                           = ref.set(Map.empty)
  }
}
