
package com.kong.eos.plugin.input.rabbitmq

import com.kong.eos.sdk.pipeline.input.Input
import org.apache.spark.streaming.rabbitmq.ConfigParameters

trait RabbitMQGenericProps {
  this: Input =>

  def propsWithStorageLevel(sparkStorageLevel: String): Map[String, String] = {
    val rabbitMQProperties = getCustomProperties
    Map(ConfigParameters.StorageLevelKey -> sparkStorageLevel) ++
      rabbitMQProperties.mapValues(value => value.toString) ++
      properties.mapValues(value => value.toString)
  }
}
