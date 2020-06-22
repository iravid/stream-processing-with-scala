package streams.workshop

import zio._
import zio.console.putStrLn
import zio.duration._
import java.nio.channels.AsynchronousFileChannel
import java.nio.file.Paths
import java.nio.ByteBuffer
import java.nio.channels.CompletionHandler
import java.nio.channels.FileChannel
import java.nio.file.Path

object Types {
  // In ZIO, the `ZIO[R, E, A]` data type represents a program that
  // requires an environment of type `R` and will either fail with
  // an error of type `E`, succeed with a value of type `A`, or never
  // terminate.

  // In this section, we will see how different types of computations
  // correspond to the various ZIO signatures.

  // type IO[E, A] = ZIO[Any, E, A]
  // type UIO[A] = ZIO[Any, Nothing, A]
  // type URIO[R, A] = ZIO[R, Nothing, A]
  // type Task[A] = ZIO[Any, Throwable, A]
  // type RIO[R, A] = ZIO[R, Throwable, A]
  // type ZIO[R, E, A]

  // trait Shape
  // trait Circle extends Shape
  // val l = List(new Circle {})
  // val l2: List[Shape] = l

  // Circle <: Shape
  // List[Circle] <: List[Shape]
  // UIO[Circle] <: UIO[Shape]

  // type Nothing // 0 values
  // type Unit // 1 value: ()

  // def returnsNothing(): Nothing = {
  //   throw new RuntimeException
  // }

  // UIO[Nothing] <: UIO[Circle] <: UIO[Shape] <: UIO[Any]

  // val doesNotFail: ZIO[Any, Nothing, Int] = UIO(2)

  // def requiresAFallibleProgram(t: Task[Int]) = ???
  // requiresAFallibleProgram(doesNotFail)
  // UIO[A] <: Task[A]
  // UIO[A] <: IO[E, A]

  // val requiresConsole: ZIO[console.Console, Throwable, Int] = ???
  // def wantsAConsoleClockProgram(pr: ZIO[console.Console with clock.Clock, Throwable, Int]) = ???
  // wantsAConsoleClockProgram(requiresConsole)

  // ZIO[console.Console with clock.Clock] >: ZIO[console.Console] >: ZIO[Any]
  // console.Console with clock.Clock <: console.Console <: Any

  // 1. A program that will succeed with an `A` or fail with an `E`.
  type FailOrSuccess[E, A] = IO[E, A]

  // 2. A program that if it terminates, yields an `A`.
  type Success[A] = UIO[A]

  // 3. A program that never terminates or fails.
  type Forever = UIO[Nothing]

  // 4. A program that never terminates, but can fail with a throwable.
  type MightThrow = Task[Nothing] // (ZIO[Any, Throwable, Nothing])

  // 5. A program that requires access to a Clock, can fail with a throwable
  // and succeeds with an `A`.
  type Program[A] = ZIO[clock.Clock, Throwable, A]

  // Blocking

  // 6. A program that requires access to blocking IO and the console,
  // could fail with a list of throwables or a number, and never terminates.
  type Last = ZIO[blocking.Blocking with console.Console, Either[List[Throwable], Int], Nothing]
}

object Values {
  // ZIO offers several ways to construct programs. In this section,
  // we will survey the essential ZIO constructors.

  // 1. A program that succeeds with the string "Hello".
  val hello: UIO[String] = UIO.succeed("Hello")

  // 2. A program that fails with the string "Boom".
  val boom: IO[String, Nothing] = IO.fail("Boom")

  // 3. A program that divides two numbers.
  def div(x: Int, y: Int): Task[Int] = Task(x / y)

  // 4. A program that prints out the requested line.
  def printIt(line: String): UIO[Unit] = UIO(println(line))

  // 5. A program that reads a line from the console.
  val readLine: UIO[String] = UIO(scala.io.StdIn.readLine())

  import java.nio.file.Files

  // 6. A program that reads a file using blocking IO.
  def readFile(name: String): ZIO[blocking.Blocking, Throwable, Array[Byte]] =
    blocking.effectBlocking(Files.readAllBytes(Paths.get(name)))

  Task.effectAsync[Int] { cb =>
    // side-effecting safe region

    new Thread {
      override def run(): Unit = {
        Thread.sleep(1000)
        cb(Task.succeed(5))
      }
    }
  }

  // 7. A program that executes the following side-effecting method,
  // and yields the results from its callbacks.
  def readFileAsync(name: String, cb: Either[Throwable, Chunk[Byte]] => Unit): Unit = {
    val channel = AsynchronousFileChannel.open(Paths.get(name))
    val buf     = ByteBuffer.allocate(channel.size().toInt)

    channel.read(
      buf,
      0L,
      (),
      new CompletionHandler[Integer, Unit] {
        def completed(read: Integer, attachment: Unit) = cb(Right(Chunk.fromByteBuffer(buf)))
        def failed(exc: Throwable, attachment: Unit)   = cb(Left(exc))
      }
    )
  }

  def readFileZIO(name: String): Task[Chunk[Byte]] =
    Task.effectAsync(cb => readFileAsync(name, res => cb(ZIO.fromEither(res))))
}

// Topics: map, flatMap, zip, zipRight, zipPar, foreach, collect
object Sequencing {
  // Programs are made of sequences of instructions. Similarly, functional
  // programs are composed of sequences of effects. In this section, we will
  // review ways to compose different effects into bigger effects.

  // 1. Use the `map` operator to convert any ZIO program that returns an
  // integer to a program that returns a string.
  def toString[R, E](zio: ZIO[R, E, Int]): ZIO[R, Throwable, String] =
    zio
      .map(i => i.toString)
      .mapError(e => new RuntimeException(s"Caught ${e.toString}"))

  val i: UIO[Int]      = UIO(2)
  def rand(lower: Int) = random.nextIntBetween(lower, 10)
  val i2               = i.flatMap(rand(_))

  val program = for {
    n <- rand(5)
    m <- rand(n)
  } yield n + m

  val program2 =
    rand(5) flatMap { n => rand(n) map { m => n + m } }

  // 2. Using a `for` comprehension, print a message for the user,
  // read a line, then print it out again.
  val askForName: UIO[Unit] =
    for {
      _    <- UIO(println("Hello there - what's your name?"))
      line <- UIO(scala.io.StdIn.readLine())
      _    <- UIO(println(s"You said: ${line}"))
    } yield ()

  val p1 = for {
    _ <- UIO(println("Hello"))
    x <- UIO(scala.io.StdIn.readLine())
  } yield x

  val p2 = UIO(println("Hello")) *> UIO(scala.io.StdIn.readLine())

  // 3. Using the `zipRight` (or `*>`) operator, print out a message 3 times.
  def printThrice(msg: String): UIO[Unit] = {
    val p = UIO(println(msg))
    p *> p *> p
  }

  val l       = UIO(2)
  val r       = UIO(3)
  val zip     = l zip r
  val zipWith = l.zipWith(r)(_ + _)
  val f       = l.flatMap(ll => r.map(rr => (ll, rr)))

  // 4. Create a ZIO program that sums the numbers from two other ZIO programs.
  // Write two versions: one with a for-comprehension, and one without.
  def sum(l: UIO[Int], r: UIO[Int]): UIO[Int] =
    for {
      ll <- l
      rr <- r
    } yield ll + rr

  def sum2(l: UIO[Int], r: UIO[Int]): UIO[Int] =
    l.zipWith(r)(_ + _)

  val rs: List[Int] = List(1, 2, 3)
  val rss           = ZIO.foreach(rs)(r => random.nextIntBetween(r, 10))

  // 5. Print out a line for every string in the list.
  def printAll(l: List[String]): ZIO[console.Console, Nothing, Unit] =
    ZIO.foreach_(l)(s => console.putStrLn(s))

  // for {
  //   // ...
  //   _ <- ZIO.foreach_(veryBigList)(expensiveOperation)

  //   // ...
  // } yield ()

  val lAndR = l <&> r

  // 6. Hash the arguments to this function in parallel, then hash
  // their concatenated hashes:
  def hash(input: String): String = {
    Thread.sleep(5000)
    input
  }

  def hashPair(left: String, right: String) =
    UIO(left).zipWithPar(UIO(right))(_ ++ _).flatMap(concatHashes => UIO(hash(concatHashes)))

  // 7. Do the same, but for a variable number of inputs:
  def hashAll(inputs: String*): UIO[String] =
    for {
      hashes      <- UIO.foreachPar(inputs)(input => UIO(hash(input)))
      hashesTwice <- UIO.foreachPar(hashes)(h => UIO(hash(h)))
      result      <- UIO(hash(hashesTwice.mkString))
    } yield result
}

object ErrorHandling {
  // val orElse = IO.fail("Boom") orElse IO.succeed("Default")
  // val catchAll = IO.fail("Boom").catchAll { e =>
  //   IO.succeed(e)
  // }
  // val folded = Task("Boom").foldM(
  //   e => IO.succeed(e.toString),
  //   v => IO.succeed(v)
  // )

  // val foldedPure = Task("Boom").fold(_.toString, v => v)

  // 1. Recover from the error in the following program by
  // printing it and returning a default value:
  val recover: ZIO[console.Console, Nothing, String] =
    IO.fail("Boom").catchAll(e => console.putStrLn(e).as(e))

  // 2. Using foldM, recover from the error by printing it and
  // returning a default, or print the value before returning it.
  def divide(i: Int, j: Int): Int = i / j
  val folded = Task(divide(5, 0))
    .foldM(
      err => console.putStrLn(err.toString).as(0),
      divideResult => console.putStrLn(divideResult.toString).as(divideResult)
    )

  sealed abstract class ProgramError(msg: String) extends Throwable(msg)
  case class Fatal(msg: String)                   extends ProgramError(msg)
  case class Retryable(msg: String)               extends ProgramError(msg)

  // 3. Using `catchSome`, recover only from retryable errors in the following program:
  val program1 = ZIO.fail(Retryable("boom"): ProgramError).catchSome {
    case Retryable(msg) => console.putStrLn(msg).as(0)
  }

  // 4. Using refineToOrDie, turn anything other than retryable errors
  // into defects:
  val program2 = ZIO.fail(Fatal("boom"): ProgramError).refineToOrDie[Retryable]

  // 5. Handle errors on the value channel with `either`:
  val mightFail: IO[ProgramError, Int] = ZIO.succeed(42)

  val either = mightFail.either

  // 6. Fallback to the secondary database if the primary fails:
  def queryFrom(database: String): Task[String]     = Task(s"result from $database")
  def queryFrom2(database: String): IO[Int, String] = ZIO.succeed(s"result from $database")

  val program3 = queryFrom("primary") orElse queryFrom2("secondary")

  // foldCauseM, foldCause, catchAllCause

  // 7. Recover from the defect in the following code which is imported as infallible:
  val lies: UIO[Int] = UIO(throw new RuntimeException)
  val noDefects      = lies.catchAllCause(_ => UIO.succeed(0))
}

object ManagedResources {
  val expensiveResource: UIO[Int] = UIO(2)
  val program                     = expensiveResource.bracket(i => UIO(println(s"releasing ${i}")))(i => UIO(println(s"Using ${i}")))

  // 1. Convert the following code into a purely functional version using bracketing:
  val fileBytes: Chunk[Byte] = {
    var channel: FileChannel = null
    try {
      channel = FileChannel.open(Paths.get("./file"))
      val buf = ByteBuffer.allocate(8192)
      channel.read(buf)
      Chunk.fromByteBuffer(buf)
    } finally {
      if (channel ne null)
        channel.close()
    }
  }

  val fileBytesBracketed: RIO[blocking.Blocking, Chunk[Byte]] = {
    val acquireFileChannel = Task(
      FileChannel.open(Paths.get("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt"))
    )

    acquireFileChannel.bracket(fileChannel => Task(fileChannel.close()).orDie) { fileChannel =>
      // interrupt
      for {
        buf <- Task(ByteBuffer.allocate(8192))
        // interrupt
        _ <- blocking.effectBlocking(fileChannel.read(buf))
        // interrupt
      } yield Chunk.fromByteBuffer(buf)
      // interrupt
    }
  }

  def acquireFileChannel(name: String) = Task(
    FileChannel.open(Paths.get(name))
  )

  // 2. Acquire channels to two files in a bracket and transfer a chunk of bytes between them:
  val transfer: Task[Unit] =
    acquireFileChannel("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt")
      .bracket(ch => Task(ch.close()).orDie) { ch1 =>
        acquireFileChannel("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt.copy")
          .bracket(ch => Task(ch.close()).orDie) { ch2 =>
            Task {
              val buf = ByteBuffer.allocate(64)
              ch1.read(buf)
              ch2.write(buf)
              ()
            }
          }
      }

  // 3. Acquire channels to *three* files in a bracket and transfer a chunk
  // of bytes from the first to the second and third:
  val transfer2: Task[Unit] =
    acquireFileChannel("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt").bracket(ch =>
      Task(ch.close()).orDie
    ) { ch1 =>
      acquireFileChannel("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt.copy")
        .bracket(ch => Task(ch.close()).orDie) { ch2 =>
          acquireFileChannel("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt.copy2")
            .bracket(ch => Task(ch.close()).orDie) { ch3 =>
              Task {
                val buf = ByteBuffer.allocate(64)
                ch1.read(buf)
                ch2.write(buf)
                ch3.write(buf)
                ()
              }
            }
        }
    }

  val resource    = ZManaged.make(UIO(println("Acquiring resource")).as(0))(i => UIO(println(s"Releasing ${i}")))
  val useResource = resource.use(i => Task(println(s"Using ${i}")))

  // 4. Create a ZManaged value that allocates a file channel.
  def fileChannel(path: String) =
    ZManaged.make {
      blocking.effectBlocking {
        FileChannel.open(Paths.get(path))
      }
    }(channel => blocking.effectBlocking(channel.close()).orDie)

  val twoFiles = fileChannel("file1") zipPar fileChannel("file2")
  // for {
  //   file1 <- fileChannel("file1")
  //   // read some data:
  //   data  <- ZManaged.make(file1.read(buf))(_ => UIO.unit)
  //   data  <- file1.read(buf).toManaged_
  //   _     <- ZManaged.finalizer(UIO(println("Before finalizing file1")))
  //   file2 <- fileChannel("file2")
  // } yield data

  // 5. Using `fileChannel`, acquire channels to all the requested paths:
  def channels(paths: String*) = ZManaged.foreach(paths)(fileChannel)
  // channels("f1", "f2", "f3")

  // val zioResource = Task(UIO(println("acquire"))).toManaged(// release)

  // 6. Compose two managed file channels in a for-comprehension, and print
  // out a message after the second one is closed:
  def channels2(p1: String, p2: String) =
    for {
      fc1 <- fileChannel(p1)
      _   <- ZManaged.finalizer(putStrLn("realeased first one"))
      fc2 <- fileChannel(p2)
    } yield (fc1, fc2)

  val fib     = console.putStrLn("ding").repeat(Schedule.fixed(1.second) && Schedule.recurs(3)).forkManaged
  val fib2    = ZManaged.fromEffect(console.putStrLn("ding").repeat(Schedule.fixed(1.second) && Schedule.recurs(3))).fork
  val printer = fib.use_(UIO.unit)

  // 7. Wrap this ZIO program with a fiber that prints out a message
  // every 5 seconds and is interrupted when the block ends:
  val monitor =
    console.putStrLn("ding").repeat(Schedule.fixed(5.second)).forkManaged.use(_ => ZIO.sleep(10.seconds) *> ZIO.unit)

  // def readFileNames =
  //   ZManaged.scope.use { scope =>
  //     for {
  //       file1 <- console.getStrLn
  //       _     <- scope(fileChannel(file1))
  //       file2 <- console.getStrLn
  //       _     <- scope(fileChannel(file2))
  //       // read from the files
  //     } yield whatEverIRead
  //   }

  // 8. Create a scope in which files can be safely opened and closed,
  // and write some data to 3 files in it:
  val safeBlock = ZManaged.scope.use { open =>
    for {
      fc1AndEarlyRelease <- open(
                             fileChannel("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt")
                           )
      (releaseFc1, fc1) = fc1AndEarlyRelease
      fc2AndEarlyRelease <- open(
                             fileChannel(
                               "/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt.copy"
                             )
                           )
      (releaseFc2, fc2) = fc2AndEarlyRelease
      buf               <- Task(ByteBuffer.allocate(4096))
      _                 <- Task(fc1.read(buf))
      _                 <- releaseFc1(Exit.unit)
      _                 <- Task(fc2.write(buf))
    } yield ()
  }

  // ZManaged.collectAll(List.fill(5)(ZManaged.scope)).use { scopes =>

  // }
  // (ZManaged.scope <*> ZManaged.scope).use { case (scope1, scope2) =>

  // }

  // 9. Test your understanding of how ZManaged works by writing out
  // the order of prints in this snippet, without running it:
  val ordering =
    for {
      _ <- ZManaged.make(putStrLn("foo"))(_ => putStrLn("foo fin"))
      _ <- ZManaged.finalizer(putStrLn("baz"))
      _ <- ZManaged.make(putStrLn("bar"))(_ => putStrLn("bar fin"))
      _ <- ZManaged.foreach(List("a", "b", "c"))(n => ZManaged.make(putStrLn(n))(_ => putStrLn(s"$n fin")))
    } yield ()

  // Order of prints:
  /*
    foo
    bar
    a
    b
    c
    c fin
    b fin 
    a fin
    bar fin
    baz
    foo fin
  */
}
