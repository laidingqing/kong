
package com.kong.eos.plugin.output.kafka

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, OutputFormatEnum, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._
import java.util.Properties

import com.kong.eos.plugin.input.kafka.KafkaBase
import com.kong.eos.sdk.properties.CustomProperties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable
import org.apache.kafka.clients.producer.ProducerConfig._

class KafkaOutput(name: String, properties: Map[String, JSerializable])
  extends Output(name, properties) with KafkaBase with CustomProperties {

  val DefaultKafkaSerializer = classOf[StringSerializer].getName
  val DefaultAck = "0"
  val DefaultBatchNumMessages = "200"
  val DefaultProducerPort = "9092"

  override val customKey = "KafkaProperties"
  override val customPropertyKey = "kafkaPropertyKey"
  override val customPropertyValue = "kafkaPropertyValue"

  val outputFormat = OutputFormatEnum.withName(properties.getString("format", "json").toUpperCase)
  val rowSeparator = properties.getString("rowSeparator", ",")

  override def supportedSaveModes: Seq[SaveModeEnum.Value] = Seq(SaveModeEnum.Append)

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)

    validateSaveMode(saveMode)

    outputFormat match {
      case OutputFormatEnum.ROW => dataFrame.rdd.foreachPartition(messages =>
        messages.foreach(message => send(tableName, message.mkString(rowSeparator))))
      case _ => dataFrame.toJSON.foreachPartition { messages =>
        messages.foreach(message => send(tableName, message))
      }
    }
  }

  def send(topic: String, message: String): Unit = {
    val record = new ProducerRecord[String, String](topic, message)
    KafkaOutput.getProducer(getProducerConnectionKey, createProducerProps).send(record)
  }

  private[kafka] def getProducerConnectionKey: String =
    getHostPort(BOOTSTRAP_SERVERS_CONFIG, DefaultHost, DefaultProducerPort)
      .getOrElse(BOOTSTRAP_SERVERS_CONFIG, throw new Exception("Invalid metadata broker list"))

  private[kafka] def createProducerProps: Properties = {
    val props = new Properties()
    properties.filter(_._1 != customKey).foreach { case (key, value) => props.put(key, value.toString) }
    mandatoryOptions.foreach { case (key, value) => props.put(key, value) }
    getCustomProperties.foreach { case (key, value) => props.put(key, value) }
    props
  }

  private[kafka] def mandatoryOptions: Map[String, String] =
    getHostPort(BOOTSTRAP_SERVERS_CONFIG, DefaultHost, DefaultProducerPort) ++
      Map(
        KEY_SERIALIZER_CLASS_CONFIG -> properties.getString(KEY_SERIALIZER_CLASS_CONFIG, DefaultKafkaSerializer),
        VALUE_SERIALIZER_CLASS_CONFIG -> properties.getString(VALUE_SERIALIZER_CLASS_CONFIG, DefaultKafkaSerializer),
        ACKS_CONFIG -> properties.getString(ACKS_CONFIG, DefaultAck),
        BATCH_SIZE_CONFIG -> properties.getString(BATCH_SIZE_CONFIG, DefaultBatchNumMessages)
      )

  override def cleanUp(options: Map[String, String]): Unit = {
    log.info(s"Closing Kafka producer in Kafka Output: $name")
    KafkaOutput.closeProducers()
  }
}

object KafkaOutput {

  private val producers: mutable.Map[String, KafkaProducer[String, String]] = mutable.Map.empty

  def getProducer(producerKey: String, properties: Properties): KafkaProducer[String, String] = {
    getInstance(producerKey, properties)
  }

  def closeProducers(): Unit = {
    producers.values.foreach(producer => producer.close())
  }

  private[kafka] def getInstance(key: String, properties: Properties): KafkaProducer[String, String] = {
    producers.getOrElse(key, {
      val producer = new KafkaProducer[String, String](properties)
      producers.put(key, producer)
      producer
    })
  }
}

