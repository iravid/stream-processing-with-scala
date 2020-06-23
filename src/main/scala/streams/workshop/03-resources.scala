package streams.workshop

import zio._
import zio.stream._
import java.nio.file.Path
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import scala.jdk.CollectionConverters._
import java.nio.file.WatchEvent

object Resources {
  // Resource management is an important part of stream processing. Resources can be
  // opened and closed throughout the stream's lifecycle, and most importantly,
  // need to be kept open for precisely as long as they are required for processing
  // the stream's data.

  class DatabaseClient(clientId: String) {
    def readRow: URIO[random.Random, String] = random.nextString(5).map(str => s"${clientId}-${str}")
    def writeRow(str: String): UIO[Unit]     = UIO(println(s"Writing ${str}"))
    def close: UIO[Unit]                     = UIO(println(s"Closing $clientId"))
  }
  object DatabaseClient {
    def make(clientId: String): Task[DatabaseClient] =
      UIO(println(s"Opening $clientId")).as(new DatabaseClient(clientId))
  }

  // 1. Create a stream that allocates the database client, reads 5 rows, writes
  // them back to the client and ends.
  val fiveRows: ??? = ZStream.empty ?

  // 2. Create a stream that reads 5 rows from 3 different database clients, and writes
  // them to a fourth (separate!) client, closing each reading client after finishing reading.
  val fifteenRows: ??? = ZStream.empty ?

  // 3. Read 25 rows from 3 different database clients, and write the rows to 5 additional
  // database clients - 5 rows each. Hint: use ZManaged.scope.
  val scopes: ??? = ZStream.empty ?
}

object FileIO {
  // 1. Implement the following combinator for reading bytes from a file using
  // java.io.FileInputStream.
  def readFileBytes(path: String): ZStream[???, ???, Byte] = ???

  // 2. Implement the following combinator for reading characters from a file using
  // java.io.FileReader.
  def readFileChars(path: String): ZStream[???, ???, Char] = ???

  // 3. Recursively enumerate all files in a directory.
  def listFilesRecursive(path: String): ZStream[???, ???, Path] = ???

  // 4. Read data from all files in a directory tree.
  def readAllFiles(path: String): ZStream[???, ???, Char] = ???

  // 5. Monitor a directory for new files using Java's WatchService.
  // Imperative example:
  def monitor(path: Path): Unit = {
    val watcher = FileSystems.getDefault().newWatchService()
    path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE)
    var cont = true

    while (cont) {
      val key = watcher.take()

      for (watchEvent <- key.pollEvents().asScala) {
        watchEvent.kind match {
          case StandardWatchEventKinds.ENTRY_CREATE =>
            val pathEv = watchEvent.asInstanceOf[WatchEvent[Path]]
            val filename = pathEv.context()

            println(s"${filename} created")
        }
      }

      cont = key.reset()
    }
  }


  def monitorFileCreation(path: String): ZStream[???, ???, Path] = ???

  // 6. Write a stream that synchronizes directories.
  def synchronize(source: String, dest: String): ??? = ???
}

object SocketIO {
  // 1. Print the first 2048 characters of the URL.
  def readUrl(url: String): ZStream[???, ???, Char] = ???

  // 2. Create an echo server with ZStream.fromSocketServer.
  val server = ZStream.fromSocketServer(???, ???)

  // 3. Use `ZStream#toInputStream` and `java.io.InputStreamReader` to decode a
  // stream of bytes from a file to a string.
  val data = ZStream.fromFile(???) ?

  // 4. Integrate GZIP decoding using GZIPInputStream, ZStream#toInputStream
  // and ZStream.fromInputStream.
  val gzipDecodingServer = ZStream.fromSocketServer(???, ???)
}
