import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration.*

import cats.effect.*
import cats.syntax.all.*
import dev.profunktor.pulsar.*
import fs2.Stream

object Main extends IOApp.Simple:
  val config = Config.Builder.default

  val topic  =
    Topic.Builder
      .withName("my-topic")
      .withConfig(config)
      .withType(Topic.Type.Persistent)
      .build

  val subs =
    Subscription.Builder
      .withName("buggy")
      .withType(Subscription.Type.Failover)
      .build

  val decoder: Array[Byte] => IO[String] = bs => IO(new String(bs, UTF_8))

  val encoder: String => Array[Byte] = _.getBytes(UTF_8)

  val handler: Throwable => IO[Consumer.OnFailure] =
    e => IO.println(e.getMessage).as(Consumer.OnFailure.Nack)

  val pSettings =
    Producer.Settings[IO, String]()
      .withShardKey(s => ShardKey.Of(s.hashCode.toString.getBytes(UTF_8)))
      .withDeduplication(SeqIdMaker.fromEq[String])

  val resources =
    for
      pulsar <- Pulsar.make[IO](config.url)
      c1 <- Consumer.make[IO, String](pulsar, topic, subs, decoder, handler)
      c2 <- Consumer.make[IO, String](pulsar, topic, subs, decoder, handler)
      p1 <- Producer.make[IO, String](pulsar, topic, encoder)
    yield (c1, c2, p1)

  def run: IO[Unit] =
    Stream
      .resource(resources)
      .flatMap { (c1, c2, p1) =>
        Stream(
          c1.autoSubscribe.evalMap(s => IO.println(s"C1: $s")),
          c2.autoSubscribe.evalMap(s => IO.println(s"C2: $s")),
          Stream.emit("foo").covary[IO].metered(1.second).evalMap(p1.send_)
        ).parJoin(3)
      }
      .interruptAfter(5.seconds)
      .compile
      .drain
