import java.nio.charset.StandardCharsets.UTF_8

import scala.concurrent.duration.*

import cats.effect.*
import cats.syntax.all.*
import dev.profunktor.pulsar.*
import dev.profunktor.pulsar.Consumer.Message as Msg
import fs2.Stream
import org.apache.pulsar.client.api.SubscriptionInitialPosition

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

  val sharedSub =
    Subscription.Builder
      .withName("buggy-ks")
      .withType(Subscription.Type.KeyShared)
      .build

  val decoder: Array[Byte] => IO[String] = bs => IO(new String(bs, UTF_8))

  val encoder: String => Array[Byte] = _.getBytes(UTF_8)

  val handler: Throwable => IO[Consumer.OnFailure] =
    e => IO.println(e.getMessage).as(Consumer.OnFailure.Nack)

  val pSettings =
    Producer.Settings[IO, String]()
      .withShardKey(s => ShardKey.Of(s.hashCode.toString.getBytes(UTF_8)))
      .withDeduplication(SeqIdMaker.fromEq)

  val cSettings =
    Consumer.Settings[IO, String]()
       .withInitialPosition(SubscriptionInitialPosition.Earliest)

  val resources =
    for
      pulsar <- Pulsar.make[IO](config.url)
      c1 <- Consumer.make[IO, String](pulsar, topic, subs, decoder, handler, cSettings)
      c2 <- Consumer.make[IO, String](pulsar, topic, subs, decoder, handler, cSettings)
      c3 <- Consumer.make[IO, String](pulsar, topic, sharedSub, decoder, handler)
      p1 <- Producer.make[IO, String](pulsar, topic, encoder, pSettings)
    yield (c1, c2, c3, p1)

  def run: IO[Unit] =
    Stream
      .resource(resources)
      .flatMap { (c1, c2, c3, p1) =>
        Stream(
          c3.autoSubscribe.evalMap(s => IO.println(s"C3: $s")),
          c1.subscribe.evalMap { case Msg(id, _, _, _, s) => IO.println(s"C1: $s") *> c1.ack(id) },
          c2.subscribe.evalMap { case Msg(id, _, _, _, s) => IO.println(s"C2: $s") *> c2.ack(id) },
          Stream.emit("test").covary[IO].metered(1.second).evalMap(p1.send_)
        ).parJoin(4)
      }
      .interruptAfter(5.seconds)
      .compile
      .drain
