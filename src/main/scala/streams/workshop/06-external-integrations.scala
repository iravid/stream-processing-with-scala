package streams.workshop

import java.util.concurrent.atomic.{ AtomicBoolean, AtomicReference }
import java.sql.{ Connection, DriverManager, ResultSet }
import java.{ util => ju }

import zio._
import zio.duration._
import zio.stream._
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Object
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import java.net.URI

object ExternalSources {
  // 1. Refactor this function, which drains a java.sql.ResultSet,
  // to use ZStream and ZManaged.
  // Type: Unbounded, stateless iteration
  def readRows(url: String, connProps: ju.Properties, sql: String): Chunk[String] = {
    var conn: Connection = null
    try {
      conn = DriverManager.getConnection(url, connProps)

      var resultSet: ResultSet = null
      try {
        val st = conn.createStatement()
        st.setFetchSize(5)
        resultSet = st.executeQuery(sql)

        val buf = mutable.ArrayBuilder.make[String]
        while (resultSet.next())
          buf += resultSet.getString(0)

        Chunk.fromArray(buf.result())
      } finally {
        if (resultSet ne null)
          resultSet.close()
      }

    } finally {
      if (conn ne null)
        conn.close()
    }
  }

  def readRowsStream(url: String, connProps: ju.Properties, sql: String) =
    ZStream.bracket(Task(DriverManager.getConnection(url, connProps)))(conn => UIO(conn.close())).flatMap { conn =>
      ZStream
        .bracket(Task {
          val st = conn.createStatement()
          st.setFetchSize(5)
          st.executeQuery(sql)
        })(rs => UIO(rs.close()))
        .flatMap { rs =>
          ZStream.repeatEffectOption(for {
            hasNext <- Task(rs.next()).mapError(Some(_))
            row <- if (!hasNext) ZIO.fail(None)
                  else Task(rs.getString(1)).mapError(Some(_))
          } yield row)
        }
    }

  val st = {
    val props = new ju.Properties()
    props.put("user", "root")
    props.put("password", "root")
    val url = "jdbc:postgresql://localhost/public"

    readRowsStream(url, props, "SELECT * FROM streams")
  }

  // 2. Convert this function, which polls a Kafka consumer, to use ZStream and
  // ZManaged.
  // Type: Unbounded, stateless iteration
  def pollConsumer(topic: String)(f: ConsumerRecord[String, String] => Unit): Unit = {
    val props = new ju.Properties
    props.put("bootstrap.server", "localhost:9092")
    props.put("group.id", "streams")
    props.put("auto.offset.reset", "earliest")
    var consumer: KafkaConsumer[String, String] = null

    try {
      consumer = new KafkaConsumer[String, String](props, new StringDeserializer, new StringDeserializer)
      consumer.subscribe(List(topic).asJava)

      while (true) consumer.poll(50.millis.asJava).forEach(f(_))
    } finally {
      if (consumer ne null)
        consumer.close()
    }
  }

  // 3. Convert this function, which enumerates keys in an S3 bucket, to use ZStream and
  // ZManaged. Bonus points for using S3AsyncClient instead.
  // Type: Unbounded, stateful iteration
  def listFiles(bucket: String, prefix: String): Chunk[S3Object] = {
    val client = S3Client
      .builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio123")))
      .endpointOverride(URI.create("http://localhost:9000"))
      .build()

    def listFilesToken(acc: Chunk[S3Object], token: Option[String]): Chunk[S3Object] = {
      val reqBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix)
      token.foreach(reqBuilder.continuationToken)
      val req  = reqBuilder.build()
      val resp = client.listObjectsV2(req)
      val data = Chunk.fromIterable(resp.contents().asScala)

      if (resp.isTruncated()) listFilesToken(acc ++ data, Some(resp.nextContinuationToken()))
      else acc ++ data
    }

    listFilesToken(Chunk.empty, None)
  }

  def listFilesStream(bucket: String, prefix: String) =
    ZStream
      .bracket(Task {
        S3Client
          .builder()
          .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minio", "minio123")))
          .endpointOverride(URI.create("http://localhost:9000"))
          .build()
      })(client => UIO(client.close()))
      .flatMap { client =>
        ZStream.paginateM(None: Option[String]) { token =>
          val reqBuilder = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix)
          token.foreach(reqBuilder.continuationToken)
          val req = reqBuilder.build()

          Task(client.listObjectsV2(req)).map { resp =>
            val data = Chunk.fromIterable(resp.contents().asScala)

            if (resp.isTruncated()) (data, Some(Some(resp.nextContinuationToken())))
            else (data, None)
          }
        }
      }
      .flattenChunks

  // 4. Convert this push-based mechanism into a stream.
  // Type: callback-based iteration
  case class Message(body: String)

  trait Subscriber {
    def onError(err: Throwable): Unit
    def onMessage(msg: Message): Unit
    def onShutdown(): Unit
  }

  trait RabbitMQ {
    def register(subscriber: Subscriber): Unit
    def shutdown(): Unit
  }

  object RabbitMQ {
    def make: RabbitMQ = new RabbitMQ {
      val subs: AtomicReference[List[Subscriber]] = new AtomicReference(Nil)
      val shouldStop: AtomicBoolean               = new AtomicBoolean(false)
      val thread: Thread = new Thread {
        override def run(): Unit = {
          while (!shouldStop.get()) {
            if (scala.util.Random.nextInt(11) < 7)
              subs.get().foreach(_.onMessage(Message(s"Hello ${System.currentTimeMillis}")))
            else
              subs.get().foreach(_.onError(new RuntimeException("Boom!")))

            Thread.sleep(1000)
          }

          subs.get().foreach(_.onShutdown())
        }
      }

      thread.run()

      def register(sub: Subscriber): Unit = {
        subs.updateAndGet(sub :: _)
        ()
      }
      def shutdown(): Unit = shouldStop.set(true)
    }
  }

  def messageStream(rabbit: RabbitMQ): ZStream[Any, Throwable, Message] =
    ZStream.effectAsync[Any, Throwable, Message] { cb =>
      rabbit.register {
        new Subscriber {
          def onError(err: Throwable) = cb(ZIO.fail(Some(err)))
          def onMessage(msg: Message) = cb(ZIO.succeed(Chunk.single(msg)))
          def onShutdown(): Unit = cb(ZIO.fail(None))
        }
      }
    }






  // 5. Convert this Reactive Streams-based publisher to a ZStream.
  val publisher = ???

}
