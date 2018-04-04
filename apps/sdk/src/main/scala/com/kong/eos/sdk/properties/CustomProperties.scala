package com.kong.eos.sdk.properties

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.properties.ValidatingPropertyMap._

trait CustomProperties {

  val customKey: String
  val customPropertyKey: String
  val customPropertyValue: String
  val properties: Map[String, JSerializable]

  def getCustomProperties: Map[String, String] =
    properties.getOptionsList(customKey, customPropertyKey, customPropertyValue).filter(_._1.nonEmpty)
}
