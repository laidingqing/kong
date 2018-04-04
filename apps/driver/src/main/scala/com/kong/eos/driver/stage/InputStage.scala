
package com.kong.eos.driver.stage

import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.policy.PhaseEnum
import com.kong.eos.serving.core.utils.ReflectionUtils
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

trait InputStage extends BaseStage {
  this: ErrorPersistor =>

  def inputStreamStage(ssc: StreamingContext, input: Input): DStream[Row] = {
    val errorMessage = s"Something gone wrong creating the input stream for: ${policy.input.get.name}."
    val okMessage = s"Stream for Input: ${policy.input.get.name} created correctly."

    generalTransformation(PhaseEnum.InputStream, okMessage, errorMessage) {
      require(policy.storageLevel.isDefined, "You need to define the storage level")
      input.initStream(ssc, policy.storageLevel.get)
    }
  }

  def createInput(ssc: StreamingContext, refUtils: ReflectionUtils): Input = {
    val errorMessage = s"Something gone wrong creating the input: ${policy.input.get.name}. Please re-check the policy."
    val okMessage = s"Input: ${policy.input.get.name} created correctly."

    generalTransformation(PhaseEnum.Input, okMessage, errorMessage) {
      require(policy.input.isDefined, "You need at least one input in your policy")
      val classType =
        policy.input.get.configuration.getOrElse(AppConstant.CustomTypeKey, policy.input.get.`type`).toString
      refUtils.tryToInstantiate[Input](classType + Input.ClassSuffix, (c) =>
        refUtils.instantiateParameterizable[Input](c, policy.input.get.configuration))
    }
  }


}
