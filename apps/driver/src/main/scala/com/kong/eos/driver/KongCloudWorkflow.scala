
package com.kong.eos.driver

import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.utils.AggregationTime
import com.kong.eos.serving.core.helpers.PolicyHelper
import com.kong.eos.serving.core.models.policy._
import com.kong.eos.serving.core.utils.CheckpointUtils
import com.kong.eos.driver.factory.SparkContextFactory._
import com.kong.eos.driver.schema.SchemaHelper
import com.kong.eos.driver.stage._
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.streaming.{Duration, StreamingContext}

class KongCloudWorkflow(val policy: PolicyModel) extends CheckpointUtils
  with InputStage with OutputStage with ParserStage with CubeStage with RawDataStage with TriggerStage
  with ZooKeeperError {

  clearError()

  private val ReflectionUtils = PolicyHelper.ReflectionUtils
  private val outputs = outputStage(ReflectionUtils)
  private var input: Option[Input] = None

  def setup(): Unit = {
    input.foreach(input => input.setUp())
    outputs.foreach(output => output.setUp())
  }

  def cleanUp(): Unit = {
    input.foreach(input => input.cleanUp())
    outputs.foreach(output => output.cleanUp())
  }

  def streamingStages(): StreamingContext = {
    clearError()

    val checkpointPolicyPath = checkpointPath(policy)
    val window = AggregationTime.parseValueToMilliSeconds(policy.sparkStreamingWindow)
    val ssc = sparkStreamingInstance(Duration(window), checkpointPolicyPath, policy.remember)
    if(input.isEmpty)
      input = Option(createInput(ssc.get, ReflectionUtils))
    val inputDStream = inputStreamStage(ssc.get, input.get)

    saveRawData(policy.rawData, inputDStream, outputs)

    policy.transformations.foreach { transformationsModel =>
      val parserSchemas = SchemaHelper.getSchemasFromTransformations(
        transformationsModel.transformationsPipe, Input.InitSchema)
      val (parsers, writerOptions) = parserStage(ReflectionUtils, parserSchemas)
      val parsedData = ParserStage.applyParsers(
        inputDStream, parsers, parserSchemas.values.last, outputs, writerOptions)

      triggersStreamStage(parserSchemas.values.last, parsedData, outputs, window)
      cubesStreamStage(ReflectionUtils, parserSchemas.values.last, parsedData, outputs)
    }

    ssc.get
  }
}

object KongCloudWorkflow {

  def apply(policy: PolicyModel): KongCloudWorkflow =
    new KongCloudWorkflow(policy)
}
