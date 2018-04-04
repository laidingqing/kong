
package com.kong.eos.driver.stage

import java.io.Serializable

import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.policy.{PhaseEnum, PolicyElementModel}
import com.kong.eos.serving.core.utils.ReflectionUtils

trait OutputStage extends BaseStage {
  this: ErrorPersistor =>

  def outputStage(refUtils: ReflectionUtils): Seq[Output] =
    policy.outputs.map(o => createOutput(o, refUtils))

  private[driver] def createOutput(model: PolicyElementModel, refUtils: ReflectionUtils): Output = {
    val errorMessage = s"Something gone wrong creating the output: ${model.name}. Please re-check the policy."
    val okMessage = s"Output: ${model.name} created correctly."
    generalTransformation(PhaseEnum.Output, okMessage, errorMessage) {
      val classType = model.configuration.getOrElse(AppConstant.CustomTypeKey, model.`type`).toString
      refUtils.tryToInstantiate[Output](classType + Output.ClassSuffix, (c) =>
        c.getDeclaredConstructor(
          classOf[String],
          classOf[Map[String, Serializable]])
          .newInstance(model.name, model.configuration)
          .asInstanceOf[Output])
    }
  }
}
