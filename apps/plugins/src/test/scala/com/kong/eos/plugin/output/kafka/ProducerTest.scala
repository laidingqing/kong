
package com.kong.eos.plugin.output.kafka

import java.io.Serializable
import java.util.Properties

import org.apache.log4j.Logger
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.util._

@RunWith(classOf[JUnitRunner])
class ProducerTest extends FlatSpec with Matchers {

  val log = Logger.getRootLogger

  val mandatoryOptions: Map[String, Serializable] = Map(
    "bootstrap.servers" -> """[{"host":"localhost","port":"9092"}]""",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "acks" -> "1",
    "batch.size" -> "200")

  val validProperties: Map[String, Serializable] = Map(
    "bootstrap.servers" -> """[{"host":"localhost","port":"9092"},{"host":"localhost2","port":"90922"}]""",
    "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    "acks" -> "all",
    "batch.size" -> 200
  )

  val noValidProperties: Map[String, Serializable] = Map(
    "bootstrap.servers" -> "",
    "key.serializer" -> "",
    "acks" -> "",
    "batch.size" -> ""
  )

  "getProducerKey" should "concatenate topic with broker list" in {
    val kafkatest = new KafkaOutput("kafka", validProperties)

    kafkatest.getProducerConnectionKey shouldBe "localhost:9092,localhost2:90922"
  }

  "getProducerKey" should "return default connection" in {
    val kafkatest = new KafkaOutput("kafka", noValidProperties)

    kafkatest.getProducerConnectionKey shouldBe "localhost:9092"
  }

  "extractOptions" should "extract mandatory options" in {
    val kafkatest = new KafkaOutput("kafka", mandatoryOptions)

    val options = kafkatest.mandatoryOptions
    options.size shouldBe 5
    options("bootstrap.servers") shouldBe "localhost:9092"
    options("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    options("value.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    options("acks") shouldBe "1"
    options("batch.size") shouldBe "200"
  }

  "extractOptions" should "extract default mandatory options when map is empty" in {

    val kafkatest = new KafkaOutput("kafka", Map.empty)

    val options = kafkatest.mandatoryOptions
    options.size shouldBe 5
    options("bootstrap.servers") shouldBe "localhost:9092"
    options("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    options("value.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    options("acks") shouldBe "0"
    options("batch.size") shouldBe "200"
  }

  "extractOptions" should "create a correct properties file" in {
    val kafkatest = new KafkaOutput("kafka", mandatoryOptions)

    val options: Properties = kafkatest.createProducerProps
    options.size shouldBe 5
    options.get("bootstrap.servers") shouldBe "localhost:9092"
    options.get("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    options.get("value.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    options.get("acks") shouldBe "1"
    options.get("batch.size") shouldBe "200"
  }

  "createProducer" should "return a valid KafkaProducer" in {
    val kafkatest = new KafkaOutput("kafka", mandatoryOptions)

    val options = kafkatest.createProducerProps
    val createProducer = Try(KafkaOutput.getProducer("key", options))

    createProducer match {
      case Success(some) => log.info("Test OK!")
      case Failure(e) => log.error("Test KO", e)
    }

    createProducer.isSuccess shouldBe true
  }
}
