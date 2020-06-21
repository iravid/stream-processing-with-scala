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

  // 1. A program that will succeed with an `A` or fail with an `E`.
  type FailOrSuccess[E, A] = ???

  // 2. A program that if it terminates, yields an `A`.
  type Success[A] = ???

  // 3. A program that never terminates or fails.
  type Forever = ???

  // 4. A program that never terminates, but can fail with a throwable.
  type MightThrow = ???

  // 5. A program that requires access to a Clock, can fail with a throwable
  // and succeeds with an `A`.
  type Program[A] = ???

  // 6. A program that requires access to blocking IO and the console,
  // could fail with a list of throwables or a number, and never terminates.
  type Last = ???
}

object Values {
  // ZIO offers several ways to construct programs. In this section,
  // we will survey the essential ZIO constructors.

  // 1. A program that succeeds with the string "Hello".
  val hello: ??? = ???

  // 2. A program that fails with the string "Boom".
  val boom: ??? = ???

  // 3. A program that divides two numbers.
  def div(x: Int, y: Int): ??? = ???

  // 4. A program that prints out the requested line.
  def printIt(line: String): ??? = ???

  // 5. A program that reads a line from the console.
  val readLine: ??? = ???

  // 6. A program that reads a file using blocking IO.
  def readFile(name: String): ZIO[???, ???, String] = ???

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

  def readFileZIO(name: String): IO[???, ???] = ???
}

// Topics: map, flatMap, zip, zipRight, zipPar, foreach, collect
object Sequencing {
  // Programs are made of sequences of instructions. Similarly, functional
  // programs are composed of sequences of effects. In this section, we will
  // review ways to compose different effects into bigger effects.

  // 1. Use the `map` operator to convert any ZIO program that returns an
  // integer to a program that returns a string.
  def toString[R, E](zio: ZIO[R, E, Int]): ZIO[R, E, String] = ???

  // 2. Using a `for` comprehension, print a message for the user,
  // read a line, then print it out again.
  val askForName: ??? = ???

  // 3. Using the `zipRight` (or `*>`) operator, print out a message 3 times.
  def printThrice(msg: String): ??? = ???

  // 4. Create a ZIO program that sums the numbers from two other ZIO programs.
  // Write two versions: one with a for-comprehension, and one without.
  def sum(l: UIO[Int], r: UIO[Int]): ??? = ???

  // 5. Print out a line for every string in the list.
  def printAll(l: List[String]): ??? = ???

  // 6. Hash the arguments to this function in parallel, then hash
  // their concatenated hashes:
  def hash(input: String): String = {
    Thread.sleep(5000)
    input
  }

  def hashPair(left: String, right: String): ??? = ???

  // 7. Do the same, but for a variable number of inputs:
  def hashAll(inputs: String*): ??? = ???
}

object ErrorHandling {
  // 1. Recover from the error in the following program by
  // printing it and returning a default value:
  val recover: ??? = IO.fail("Boom") ?

  // 2. Using foldM, recover from the error by printing it and
  // returning a default, or print the value before returning it.
  def divide(i: Int, j: Int): Int = i / j
  val folded: ???                 = Task(divide(5, 0)) ?

  sealed abstract class ProgramError(msg: String) extends Throwable(msg)
  case class Fatal(msg: String)                   extends ProgramError(msg)
  case class Retryable(msg: String)               extends ProgramError(msg)

  // 3. Recover only from retryable errors in the following program:
  val program1: ??? = ZIO.fail(Retryable("boom")) ?

  // 4. Using refineToOrDie, turn anything other than retryable errors
  // into defects:
  val program2: ??? = ZIO.fail(Fatal("boom")) ?

  // 5. Handle errors on the value channel with `either`:
  val mightFail: IO[ProgramError, Int] = ZIO.succeed(42)

  val either: ??? = mightFail ?

  // 6. Fallback to the secondary database if the primary fails:
  def queryFrom(database: String): Task[String] = Task(s"result from $database")

  val program3: ??? = queryFrom("primary") ?

  // 7. Recover from the defect in the following code which is imported as infallible:
  val lies: UIO[Int] = UIO(throw new RuntimeException)
  val noDefects: ??? = lies ?
}

object ManagedResources {
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

  // 2. Acquire channels to two files in a bracket and transfer a chunk of bytes between them:
  val transfer: Task[Unit] = ???

  // 3. Acquire channels to *three* files in a bracket and transfer a chunk
  // of bytes from the first to the second and third:
  val transfer2: Task[Unit] = ???

  // 4. Create a ZManaged value that allocates a file channel.
  def fileChannel(path: Path): ??? = ???

  // 5. Using `fileChannel`, acquire channels to all the requested paths:
  def channels(paths: Path*): ??? = ???

  // 6. Compose two managed file channels in a for-comprehension, and print
  // out a message after the second one is closed:
  val channels2: ??? = ???

  // 7. Wrap this ZIO program with a fiber that prints out a message
  // every 5 seconds and is interrupted when the block ends:
  val monitor: ??? = ZIO.sleep(10.seconds) ?

  // 8. Create a scope in which files can be safely opened and closed,
  // and write some data to 3 files in it:
  val safeBlock: ??? = ???

  // 9. Test your understanding of how ZManaged works by writing out
  // the order of prints in this snippet, without running it:
  val ordering =
    for {
      _ <- ZManaged.make(putStrLn("foo"))(_ => putStrLn("foo fin"))
      _ <- ZManaged.finalizer(putStrLn("baz"))
      _ <- ZManaged.make(putStrLn("bar"))(_ => putStrLn("bar fin"))
      _ <- ZManaged.foreach(List("a", "b", "c"))(n => ZManaged.make(putStrLn(n))(_ => putStrLn(s"$n fin")))
    } yield ()
}
