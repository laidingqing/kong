
package com.kong.eos.serving.core.models.policy

import java.util.Date

object PhaseEnum extends Enumeration {
  val Input = Value("Input")
  val InputStream = Value("InputStream")
  val Parser = Value("Parser")
  val Operator = Value("Operator")
  val Cube = Value("Cube")
  val CubeStream = Value("CubeStream")
  val Output = Value("Output")
  val Trigger = Value("Trigger")
  val TriggerStream = Value("TriggerStream")
  val Execution = Value("Execution")
  val RawData = Value("RawData")
}

case class PolicyErrorModel(
                             message: String,
                             phase: PhaseEnum.Value,
                             originalMsg: String,
                             date: Date = new Date
                           )
