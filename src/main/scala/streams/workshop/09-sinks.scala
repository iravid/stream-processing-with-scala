package streams.workshop

import zio._
import zio.stream._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object Sinks {
  // 1. Extract the first element of this stream using runHead.
  val head = ZStream.unwrap(random.nextInt.map(ZStream.range(0, _))) ?

  // 2. Extract the last element of this stream using runLast.
  val last = ZStream.unwrap(random.nextInt.map(ZStream.range(0, _))) ?

  // 3. Parse the CSV file at the root of the repository into its header line
  // and a stream that represents the rest of the lines. Use `ZStream#peel`.
  val peel = ZStream.fromFile(???) ?

  // 4. Transduce the bytes read from a file into lines and print them out;
  // sum the amount of bytes that were read from the file and print that out
  // when the stream ends.
  // Use: ZStream#tap for writing, ZSink.count+contramap for summing
  val sumBytes = ZStream.fromFile(???) ?

  // 5. Use ZSink.managed to wrap a Kafka producer and write every line
  // from a file to a topic.
  def producer: ZManaged[Any, Throwable, KafkaProducer[String, String]] = {
    val props = new java.util.Properties
    props.put("bootstrap.server", "localhost:9092")
    ZManaged.makeEffect(new KafkaProducer(props, new StringSerializer, new StringSerializer))(_.close())
  }

  def toProducerRecord(topic: String, value: String): ProducerRecord[String, String] =
    new ProducerRecord[String, String](topic, value)

  def pipeFileToTopic(file: String, topic: String) = ???

  // 6. Combine the sink from `sumBytes` and the sink from `pipeFileToTopic` into
  // a single sink that both writes to Kafka and counts how many bytes were written
  // using `zipParRight`.

  // 7. Use ZSink.fold to sample 10 random elements from the stream, and then switch
  // to ZSink.collectAllToSet to gather the rest of the elements of the stream.
  val sequencedSinks = ZStream.repeatEffect(random.shuffle(List("a", "b", "c", "d")).map(_.head))

  // 8. Use ZSink.fromOutputStream, GZIPOutputStream and ZStream.fromFile to compress a file.
  def compressFile(filename: String) = ???
}
