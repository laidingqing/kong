
package com.kong.eos.plugin.input.rabbitmq

import java.io.{Serializable => JSerializable}

import com.kong.eos.plugin.input.rabbitmq.handler.MessageHandler
import com.kong.eos.sdk.pipeline.input.Input
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.rabbitmq.distributed.RabbitMQDistributedKey
import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import scala.util.Try
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils._

object RabbitMQDistributedInput {
  //Keys from UI
  val DistributedPropertyKey = "distributedProperties"
  val QueuePropertyKey = "distributedQueue"
  val ExchangeNamePropertyKey = "distributedExchangeName"
  val ExchangeTypePropertyKey = "distributedExchangeType"
  val RoutingKeysPropertyKey = "distributedRoutingKeys"
  val HostPropertyKey = "hosts"

  //Default values
  val QueueDefaultValue = "queue"
  val HostDefaultValue = "localhost"
}

class RabbitMQDistributedInput(properties: Map[String, JSerializable])
  extends Input(properties) with RabbitMQGenericProps {

  import RabbitMQDistributedInput._


  def initStream(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {
    val messageHandler = MessageHandler(properties).handler
    val params = propsWithStorageLevel(sparkStorageLevel)
    createDistributedStream(ssc, getKeys(params), params, messageHandler)
  }

  def getKeys(rabbitMQParams: Map[String, String]): Seq[RabbitMQDistributedKey] = {
    val items = Try(properties.getMapFromJsoneyString(DistributedPropertyKey))
      .getOrElse(Seq.empty[Map[String, String]])
    for (item <- items) yield getKey(item, rabbitMQParams)
  }

  def getKey(params: Map[String, String], rabbitMQParams: Map[String, String]): RabbitMQDistributedKey = {
    val exchangeAndRouting = ExchangeAndRouting(
      params.get(ExchangeNamePropertyKey).notBlank,
      params.get(ExchangeTypePropertyKey).notBlank,
      params.get(RoutingKeysPropertyKey).notBlank
    )
    val hosts = HostPropertyKey -> params.get(HostPropertyKey).notBlankWithDefault(HostDefaultValue)
    val queueName = params.get(QueuePropertyKey).notBlankWithDefault(QueueDefaultValue)

    RabbitMQDistributedKey(
      queueName,
      exchangeAndRouting,
      rabbitMQParams + hosts
    )
  }
}
