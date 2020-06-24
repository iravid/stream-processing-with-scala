package streams.workshop

import zio._
import zio.stream._
import java.nio.file.Path
import java.nio.file.FileSystems
import java.nio.file.StandardWatchEventKinds
import scala.jdk.CollectionConverters._
import java.nio.file.WatchEvent
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.net.URL
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream

object Resources {
  // Resource management is an important part of stream processing. Resources can be
  // opened and closed throughout the stream's lifecycle, and most importantly,
  // need to be kept open for precisely as long as they are required for processing
  // the stream's data.

  // val stream = ZStream.bracket(console.putStrLn("Acquiring").as(5))(i => console.putStrLn(s"Releasing ${i}"))
  //   .flatMap { acquiredResource =>
  //     // resource lifetime
  //     ZStream.repeatEffect(console.putStrLn(s"Using resource ${acquiredResource}"))
  //       .mapConcat()
  //   }

  // val stream = ZStream.managed(ZManaged.make(console.putStrLn("Acquiring").as(5))(i => console.putStrLn(s"Releasing ${i}")))
  //   .flatMap { acquiredResource =>

  //   }

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
  val fiveRows = ZStream
    .bracket(DatabaseClient.make("id-here"))(_.close)
    .flatMap(client =>
      ZStream
        .repeatEffect(client.readRow)
        .take(5)
        .tap(client.writeRow)
    )

  // 2. Create a stream that reads 5 rows from 3 different database clients, and writes
  // them to a fourth (separate!) client, closing each reading client after finishing reading.
  def readDatabase(clientId: String, nRows: Int) =
    ZStream.bracket(DatabaseClient.make(clientId))(_.close).flatMap { client =>
      ZStream.repeatEffect(client.readRow).take(nRows.toLong)
    }
  val fifteenRows =
    ZStream.bracket(DatabaseClient.make("writing-client"))(_.close).flatMap { writingClient =>
      ZStream
        .concatAll(
          Chunk(
            readDatabase("one", 5),
            readDatabase("two", 5),
            readDatabase("three", 5)
          )
        )
        .tap(writingClient.writeRow)
    }

  // 3. Read 25 rows from 3 different database clients, and write the rows to 5 additional
  // database clients - 5 rows each. Hint: use ZManaged.scope.
  def makeManagedClient(clientId: String) = DatabaseClient.make(clientId).toManaged(_.close)

  //      10            10      5       <- reading clients
  // | --------- | --------- | --- |
  //
  // |---|---|---|---|...               <- writing clients
  //

  val scopes = ZStream.fromEffect(Ref.make(0)).flatMap { rowsRead =>
    ZStream.managed(ZManaged.scope).flatMap { openWriter =>
      ZStream.fromEffect(openWriter(makeManagedClient("initial"))).flatMap {
        case (initFinalizer, initClient) =>
          ZStream.fromEffect(Ref.make((initFinalizer, initClient))).flatMap { clientRef =>
            ZStream
              .concatAll(
                Chunk(
                  readDatabase("one", 10),
                  readDatabase("two", 10),
                  readDatabase("three", 5)
                )
              )
              .tap { row =>
                val writeRow = clientRef.get.flatMap(_._2.writeRow(row))
                val switchOut = clientRef.get.flatMap(_._1(Exit.unit)) *>
                  openWriter(makeManagedClient("next")).flatMap {
                    case (nextFinalizer, nextClient) =>
                      clientRef.set(nextFinalizer -> nextClient) *> nextClient.writeRow(row)
                  }

                rowsRead.modify { currRows =>
                  if (currRows == 5) (switchOut, 0)
                  else (writeRow, currRows + 1)
                }.flatten
              }
          }
      }
    }
  }

  val forComp = for {
    rowsRead         <- ZStream.fromEffect(Ref.make(0))
    openWriter       <- ZStream.managed(ZManaged.scope)
    initFinAndClient <- ZStream.fromEffect(openWriter(makeManagedClient("initial")))
    clientRef        <- ZStream.fromEffect(Ref.make((initFinAndClient._1, initFinAndClient._2)))
    row <- ZStream
            .concatAll(
              Chunk(
                readDatabase("one", 10),
                readDatabase("two", 10),
                readDatabase("three", 5)
              )
            )
            .tap { row =>
              val writeRow = clientRef.get.flatMap(_._2.writeRow(row))
              val switchOut = clientRef.get.flatMap(_._1(Exit.unit)) *>
                openWriter(makeManagedClient("next")).flatMap {
                  case (nextFinalizer, nextClient) =>
                    clientRef.set(nextFinalizer -> nextClient) *> nextClient.writeRow(row)
                }

              rowsRead.modify { currRows =>
                if (currRows == 5) (switchOut, 0)
                else (writeRow, currRows + 1)
              }.flatten
            }
  } yield row
}

object FileIO {
  // 1. Implement the following combinator for reading bytes from a file using
  // java.io.FileInputStream.
  def readFileBytes(path: String): ZStream[blocking.Blocking, IOException, Byte] =
    ZStream.fromInputStreamEffect(ZIO(new FileInputStream(path)).refineToOrDie[IOException])

  // 2. Implement the following combinator for reading characters from a file using
  // java.io.FileReader.
  def readFileChars(path: String): ZStream[blocking.Blocking, Throwable, Char] =
    ZStream.bracket(ZIO(new java.io.FileReader(path)))(reader => UIO(reader.close())).flatMap { reader =>
      ZStream.repeatEffectChunkOption {
        for {
          buf       <- UIO(Array.ofDim[Char](2048))
          charsRead <- blocking.effectBlocking(reader.read(buf)).mapError(Some(_))
          result <- if (charsRead == -1) ZIO.fail(None)
                   else UIO(Chunk.fromArray(buf.slice(0, charsRead)))
        } yield result
      }
    }

  // 3. Recursively enumerate all files in a directory.
  def listFilesRecursive(path: String): ZStream[console.Console, Throwable, Path] =
    ZStream
      .bracket(ZIO(Files.walk(Paths.get(path))))(st => UIO(st.close()))
      .flatMap(ZStream.fromJavaStream(_))
      .tap(path => console.putStrLn(path.toString()))

  // 4. Read data from all files in a directory tree.
  def readAllFiles(path: String) =
    listFilesRecursive(path)
      .filter(Files.isRegularFile(_))
      .take(2)
      .flatMap(path => readFileChars(path.toString))

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
            val pathEv   = watchEvent.asInstanceOf[WatchEvent[Path]]
            val filename = pathEv.context()

            println(s"${filename} created")
        }
      }

      cont = key.reset()
    }
  }

  def monitorFileCreation(path: String): ZStream[Any, Throwable, Path] =
    ZStream.managed(ZManaged.fromAutoCloseable(ZIO(FileSystems.getDefault().newWatchService()))).flatMap { watcher =>
      ZStream.fromEffect(ZIO(Paths.get(path).register(watcher, StandardWatchEventKinds.ENTRY_CREATE))).drain ++
        ZStream.repeatEffectChunkOption {
          for {
            key <- ZIO(watcher.take()).mapError(Some(_))
            events <- ZIO(
                       key
                         .pollEvents()
                         .asScala
                         .filter(_.kind() == StandardWatchEventKinds.ENTRY_CREATE)
                         .map { watchEvent =>
                           val pathEv   = watchEvent.asInstanceOf[WatchEvent[Path]]
                           val filename = pathEv.context()

                           filename
                         }
                     ).mapError(Some(_))
            continue <- ZIO(key.reset()).mapError(Some(_))
            _        <- ZIO.fail(None).when(!continue)
          } yield Chunk.fromIterable(events)
        }
    }

  // 6. Write a stream that synchronizes directories.
  def synchronize(source: String, dest: String) =
    monitorFileCreation(source).mapM { p =>
      console.putStrLn(s"Copying ${p}") *>
        ZIO(
          Files.copy(
            Path.of(source, p.toString),
            Path.of(dest, p.getFileName.toString),
            StandardCopyOption.REPLACE_EXISTING
          )
        ).refineToOrDie[IOException]
    }
}

object SocketIO {
  def fromReaderEffect(reader: Task[java.io.Reader]): ZStream[blocking.Blocking, Throwable, Char] =
    ZStream.bracket(reader)(reader => UIO(reader.close())).flatMap { reader =>
      ZStream.repeatEffectChunkOption {
        for {
          buf       <- UIO(Array.ofDim[Char](2048))
          charsRead <- blocking.effectBlocking(reader.read(buf)).mapError(Some(_))
          result <- if (charsRead == -1) ZIO.fail(None)
                   else UIO(Chunk.fromArray(buf.slice(0, charsRead)))
        } yield result
      }
    }

  // 1. Print the first 2048 characters of the URL.
  def readUrl(url: String) =
    fromReaderEffect(ZIO(new java.io.InputStreamReader(new URL(url).openStream)))
      .take(2048)
      // chunking
      // write this to a file
      // read multiple urls at the same time
      .tap(c => console.putStrLn(c.toString))

  def businessLogic[R, E](stream: ZStream[R, E, Char]) =
    stream.take(2048).tap(c => console.putStrLn(c.toString))

  businessLogic(ZStream('a', 'b', 'c'))

  // 2. Create a server that prints out data from incoming connections with ZStream.fromSocketServer.
  val server = ZStream.fromSocketServer(8080).flatMap { connection =>
    connection.read.tap(byte => console.putStrLn(byte.toChar.toString))
  }

  val st: ZStream[Any, Throwable, Byte] = ???
  val is                                = st.toInputStream

  // 3. Use `ZStream#toInputStream` and `java.io.InputStreamReader` to decode a
  // stream of bytes from a file to a stream of chars.
  val data =
    ZStream
      .managed(
        ZStream
          .fromFile(Paths.get("/Users/iravid/Development/ziverge/stream-processing-with-scala/build.sbt"))
          .toInputStream
      )
      .flatMap(is => fromReaderEffect(Task(new InputStreamReader(is))))
      .take(16)
      .tap(c => console.putStrLn(c.toString))

  // 4. Integrate GZIP decoding using GZIPInputStream, ZStream#toInputStream
  // and ZStream.fromInputStream.
  val gzipDecodingServer = ZStream.fromSocketServer(8080, Some("localhost")).flatMap { connection =>
    ZStream.managed(connection.read.toInputStream).flatMap { inputStream =>
      ZStream.fromInputStream(new GZIPInputStream(inputStream))
    }
  }
}
