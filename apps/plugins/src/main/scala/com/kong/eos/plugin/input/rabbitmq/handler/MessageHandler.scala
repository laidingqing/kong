
package com.kong.eos.plugin.input.rabbitmq.handler

import java.io.{Serializable => JSerializable}

import com.rabbitmq.client.QueueingConsumer.Delivery
import org.apache.spark.sql.Row
import com.kong.eos.sdk.properties.ValidatingPropertyMap._

sealed abstract class MessageHandler {
  def handler: Delivery => Row
}

object MessageHandler {
  val KeyDeserializer = "key.deserializer"
  val DefaultSerializer = "string"
  val ArraySerializerKey = "arraybyte"

  def apply(properties: Map[String, JSerializable]): MessageHandler = {
    apply(properties.getString(KeyDeserializer, DefaultSerializer))
  }

  def apply(handlerType: String): MessageHandler = handlerType match {
    case ArraySerializerKey => ByteArrayMessageHandler
    case _ => StringMessageHandler
  }

  case object StringMessageHandler extends MessageHandler {
    override def handler: (Delivery) => Row = (rawMessage: Delivery) => Row(new Predef.String(rawMessage.getBody))
  }

  case object ByteArrayMessageHandler extends MessageHandler {
    override def handler: (Delivery) => Row = (rawMessage: Delivery) => Row(rawMessage.getBody)
  }
}
