package streams.workshop

import zio._
import zio.stream._

object Chunks {
  val c  = Chunk.fromIterable(List(1, 2, 3))
  val c1 = Chunk.single(1)
  val c2 = Chunk(2)

  // 1. Create a chunk of integers from an array.
  val intChunk: Chunk[Int] = Chunk.fromArray(Array.fill(10)(10))

  // 2. Fold a chunk of integers into its sum.
  val sum: Int = Chunk(1, 2, 3, 4, 5).foldLeft(0)(_ + _)

  // 3. Fold a chunk of integers into its sum and print the
  // partial sums as it is folded.
  val sum2: URIO[console.Console, Int] =
    Chunk(1, 2, 3).foldM(0)((sum, el) => console.putStrLn(sum.toString).as(sum + el))

  // 4. Copy the contents of a chunk to an array.
  val arr: Array[Int] = Chunk(1, 2, 3).toArray

  // 5. Incrementally build a chunk using a ChunkBuilder.
  val builder = ChunkBuilder.make[Int]()
  builder += 1
  val result = builder.result()

  val buildChunk: Chunk[Int] = {
    val builder = ChunkBuilder.make[Int]()
    List.fill(10)(10).foreach(builder += _)
    builder.result()
  }
}

class PrimitiveBoxing {
  // Compare the bytecode resulting from each test here.
  def test0(): Int = {
    val vec = Vector(1, 2, 3)

    val i1 = vec(1)

    i1
  }

  def test1(): Int = {
    val chunk = Chunk(1, 2, 3)

    val i1 = chunk(1)

    i1
  }

  def test2(): Byte = {
    val chunk = Chunk[Byte](1, 2, 3)

    val i1 = chunk.byte(1)
    // 1. Convert to an array
    // 2. Use a while loop and Chunk.byte/int/float ...
    // 3. Use the Chunk.bytes/ints/floats: Array[Byte/Int/Float]

    i1
  }

  // u8 zip u16 <= invertible
  // => JVM ByteCode
  // => Compile that at runtime
  // => Run it
}

object ChunkedStreams {
  // 1. Create a stream from a chunk.
  val intStream: Stream[Nothing, Int] = ZStream.fromChunk(Chunk(1, 2, 3))

  // 2. Create a stream from multiple chunks.
  val intStream2: Stream[Nothing, Int] = ZStream.fromChunks(Chunk(1), Chunk(2, 3))

  // 3. Consume the chunks of intStream2 and print them out.
  val printChunks: URIO[console.Console, Unit] =
    intStream2.foreachChunk(chunk => console.putStrLn(chunk.toString))

  // 4. Transform each chunk of integers to its sum and print it.
  val summedChunks =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4), Chunk(5, 6))
      .mapChunks(c => Chunk.single(c.sum))
      .foreachChunk(c => console.putStrLn(c.toString))

  // 5. Compare the chunking behavior of mapChunksM and mapM.
  val printedNumbers: ZStream[console.Console, Nothing, Int] =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4))
      .mapM(i => console.putStrLn(i.toString).as(i))

  // Chunk structure: singleton chunks

  val printedNumbersChunk: ZStream[console.Console, Nothing, Int] =
    ZStream
      .fromChunks(Chunk(1, 2), Chunk(3, 4))
      .mapChunksM(_.mapM(i => console.putStrLn(i.toString).as(i)))

  // Chunk structure: original chunks

  // 6. Compare the behavior of the following streams under errors.
  val mappedStream =
    ZStream
      .fromChunks(Chunk(1, 2, 3, 8, 9, 10, 11))
      .mapM { i =>
        if (i < 8) Task.succeed(i)
        else Task.fail(new RuntimeException("Boom"))
      }
      .tap(i => console.putStrLn(i.toString))

  val mappedChunkStream =
    ZStream(1, 2, 3, 8, 9, 10, 11).mapChunksM { is =>
      is.mapM { i =>
        if (i < 8) Task.succeed(i)
        else Task.fail(new RuntimeException("Boom"))
      }
    }.tap(i => console.putStrLn(i.toString))

  // 7. Re-chunk a singleton chunk stream into chunks of 4 elements.
  val rechunked =
    ZStream.fromChunks(List.fill(8)(Chunk(1)): _*).chunkN(6).foreachChunk(c => console.putStrLn(c.toString))

  // 8. Build a stream of longs from 0 to Int.MaxValue + 3.
  val longs: Stream[Nothing, Long] =
    ???

  // 9. Flatten this stream of chunks:
  val chunks: Stream[Nothing, Int] = ZStream(Chunk(1, 2, 3), Chunk(4, 5)).flattenChunks
}
