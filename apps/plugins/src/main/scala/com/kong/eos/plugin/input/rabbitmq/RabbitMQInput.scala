
package com.kong.eos.plugin.input.rabbitmq

import java.io.{Serializable => JSerializable}

import com.kong.eos.plugin.input.rabbitmq.handler.MessageHandler
import com.kong.eos.sdk.pipeline.input.Input
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.rabbitmq.RabbitMQUtils._

class RabbitMQInput(properties: Map[String, JSerializable])
  extends Input(properties) with RabbitMQGenericProps {

  def initStream(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {
    val messageHandler = MessageHandler(properties).handler
    val params = propsWithStorageLevel(sparkStorageLevel)
    createStream(ssc, params, messageHandler)
  }


}
