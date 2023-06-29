package pt.davidafsilva.vertx

import io.vertx.core.AbstractVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.impl.ContextInternal
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.LocalDateTime
import java.util.Scanner
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

fun main() {
  val waitFlag = AtomicBoolean(true)
  val vertx = Vertx.vertx()
  vertx.deployEventBusKafkaPublisherVerticle(waitFlag)

  println("Supported operations:")
  println(" [1] Send quickly processed message to verticle")
  println(" [2] Send slowly processed message to verticle")
  println(" [3] Short-circuit ongoing 'slow' operation")
  println(" [0] Exit")
  println("")
  println("Type the numerical value for the desired operation and press <enter>")

  val scanner = Scanner(System.`in`)
  while (true) {
    when (scanner.next()) {
      "1" -> vertx.eventBus().send("publisher", "quick")
      "2" -> vertx.eventBus().send("publisher", "slow")
      "3" -> waitFlag.set(false)
      "0" -> break
      else -> println("err: invalid opcode")
    }
    println()
  }

  println("closing Vert.x..")
  vertx.close()
}

private fun Vertx.deployEventBusKafkaPublisherVerticle(waitFlag: AtomicBoolean) {
  val options = DeploymentOptions()
    .setWorker(true)
    .setWorkerPoolName("kafka-verticle-worker-pool")
    .setWorkerPoolSize(2)
  val verticle = EventBusKafkaPublisherVerticle(
    ebAddress = "publisher",
    topic = "topic",
    waitFlag = waitFlag,
  )
  deployVerticle(verticle, options)
    .onFailure { ex -> log("deployment failed: $ex") }
    .toCompletionStage()
    .toCompletableFuture()
    .get(10, TimeUnit.SECONDS)
}

class EventBusKafkaPublisherVerticle(
  private val ebAddress: String,
  private val topic: String,
  private val waitFlag: AtomicBoolean,
) : AbstractVerticle() {

  override fun start(startPromise: Promise<Void>) {
    ebConsumer()
    kafkaConsumer().onComplete(startPromise)
  }

  private fun ebConsumer() {
    val config = mapOf(
      BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      CLIENT_ID_CONFIG to "test",
      ProducerConfig.ACKS_CONFIG to "all",
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.canonicalName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.canonicalName,
    )
    val producer = KafkaProducer.create<String, String>(vertx, config)
    val keyGen = AtomicInteger()
    vertx.eventBus().localConsumer(ebAddress) { m ->
      log("EventBus - message received: ${m.body()}")
      producer.send(KafkaProducerRecord.create(topic, "${keyGen.incrementAndGet()}", m.body()))
    }
  }

  private fun kafkaConsumer(): Future<Void> {
    val config = mapOf(
      BOOTSTRAP_SERVERS_CONFIG to "localhost:9092",
      ENABLE_AUTO_COMMIT_CONFIG to "false",
      AUTO_OFFSET_RESET_CONFIG to "latest",
      GROUP_ID_CONFIG to "test",
      CLIENT_ID_CONFIG to "test",
      KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.canonicalName,
      VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.canonicalName,
    )

    return KafkaConsumer.create<String, String>(vertx, config)
      .handler { r ->
        log("KafkaConsumer - record received: ${r.key()}/${r.value()}")
        if (r.value() == "slow") {
          while (!waitFlag.compareAndSet(false, true)) {
            Thread.sleep(500)
          }
        }
        log("KafkaConsumer - record processed: ${r.key()}/${r.value()}")
      }
      .subscribe(topic)
  }
}

private fun log(msg: String) {
  val ctx = ContextInternal.current().unwrap()
//    val trace = StringWriter().use { w ->
//        Exception("call trace").printStackTrace(PrintWriter(w))
//        w.toString()
//    }
  println("[${Thread.currentThread().name}][$ctx] ${LocalDateTime.now()} - $msg")
//    println("[${Thread.currentThread().name}][$ctx] ${now()} - $msg\n$trace")
}
