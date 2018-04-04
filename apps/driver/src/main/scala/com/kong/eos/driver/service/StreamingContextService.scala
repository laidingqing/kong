
package com.kong.eos.driver.service

import java.io.File

import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.serving.core.constants.AppConstant._
import com.kong.eos.serving.core.helpers.PolicyHelper
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.utils.{CheckpointUtils, SchedulerUtils}
import com.kong.eos.driver.KongCloudWorkflow
import com.kong.eos.driver.factory.SparkContextFactory._
import com.kong.eos.driver.utils.LocalListenerUtils
import com.mongodb.casbah.MongoClient
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.streaming.StreamingContext

import scala.util.Try

case class StreamingContextService(generalConfig: Option[Config] = None)
  extends SchedulerUtils with CheckpointUtils with LocalListenerUtils {

  def localStreamingContext(policy: PolicyModel, files: Seq[File]): (KongCloudWorkflow, StreamingContext) = {
    killLocalContextListener(policy, policy.name)

    if (autoDeleteCheckpointPath(policy)) deleteCheckpointPath(policy)

    createLocalCheckpointPath(policy)

    val outputsSparkConfig =
      PolicyHelper.getSparkConfigs(policy.outputs, Output.SparkConfigurationMethod, Output.ClassSuffix)
    val policySparkConfig = PolicyHelper.getSparkConfigFromPolicy(policy)
    val propsConfig = Try(PolicyHelper.getSparkConfFromProps(generalConfig.get.getConfig(ConfigLocal)))
      .getOrElse(Map.empty[String, String])

    sparkStandAloneContextInstance(propsConfig ++ policySparkConfig ++ outputsSparkConfig, files)

    val spartaWorkflow = KongCloudWorkflow(policy)
    val ssc = spartaWorkflow.streamingStages()

    setSparkContext(ssc.sparkContext)
    setSparkStreamingContext(ssc)
    setInitialSentences(policy.initSqlSentences.map(modelSentence => modelSentence.sentence))

    (spartaWorkflow, ssc)
  }

  def clusterStreamingContext(policy: PolicyModel, files: Seq[String]): (KongCloudWorkflow, StreamingContext) = {
    val spartaWorkflow = KongCloudWorkflow(policy)

    val ssc = StreamingContext.getOrCreate(checkpointPath(policy), () => {
      log.info(s"Nothing in checkpoint path: ${checkpointPath(policy)}")
      val outputsSparkConfig =
        PolicyHelper.getSparkConfigs(policy.outputs, Output.SparkConfigurationMethod, Output.ClassSuffix)
      sparkClusterContextInstance(outputsSparkConfig, files)
      spartaWorkflow.streamingStages()
    })

    setSparkContext(ssc.sparkContext)
    setSparkStreamingContext(ssc)
    setInitialSentences(policy.initSqlSentences.map(modelSentence => modelSentence.sentence))

    (spartaWorkflow, ssc)
  }
}