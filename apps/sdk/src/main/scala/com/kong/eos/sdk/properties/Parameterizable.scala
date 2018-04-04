package com.kong.eos.sdk.properties

import java.io.Serializable

abstract case class Parameterizable(properties: Map[String, Serializable]) {

  require(Option(properties).isDefined, "The properties map cannot be null")
}
