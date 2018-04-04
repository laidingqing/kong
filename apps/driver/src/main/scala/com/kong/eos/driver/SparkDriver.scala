
package com.kong.eos.driver

import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.helpers.ResourceManagerLinkHelper
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum._
import com.kong.eos.serving.core.models.policy.{PhaseEnum, PolicyErrorModel, PolicyStatusModel}
import com.kong.eos.serving.core.utils._
import com.google.common.io.BaseEncoding
import com.kong.eos.driver.exception.DriverException
import com.kong.eos.driver.service.StreamingContextService
import com.typesafe.config.ConfigFactory
import org.apache.curator.framework.CuratorFramework

import scala.util.{Failure, Properties, Success, Try}

object SparkDriver extends PluginsFilesUtils {

  val NumberOfArguments = 6
  val ClusterConfigIndex = 0
  val DetailConfigurationIndex = 1
  val PluginsFilesIndex = 2
  val PolicyIdIndex = 3
  val DriverLocationConfigIndex = 4
  val ZookeeperConfigurationIndex = 5
  val JaasConfEnv = "SPARTA_JAAS_FILE"

  //scalastyle:off
  def main(args: Array[String]): Unit = {
    assert(args.length == NumberOfArguments,
      s"Invalid number of arguments: ${args.length}, args: $args, expected: $NumberOfArguments")
    Try {
      Properties.envOrNone(JaasConfEnv).foreach(jaasConf => {
        log.info(s"Adding java security conf file: $jaasConf")
        System.setProperty("java.security.auth.login.config", jaasConf)
      })
      val policyId = args(PolicyIdIndex)
      val detailConf = new String(BaseEncoding.base64().decode(args(DetailConfigurationIndex)))
      val zookeeperConf = new String(BaseEncoding.base64().decode(args(ZookeeperConfigurationIndex)))
      val pluginsFiles = new String(BaseEncoding.base64().decode(args(PluginsFilesIndex)))
        .split(",").filter(s => s != " " && s.nonEmpty)
      val driverLocationConf = new String(BaseEncoding.base64().decode(args(DriverLocationConfigIndex)))
      val clusterConf = new String(BaseEncoding.base64().decode(args(ClusterConfigIndex)))

      initSpartaConfig(detailConf, zookeeperConf, driverLocationConf, clusterConf)

      val policyStatusUtils = new PolicyStatusUtils {

      }
      Try {
        addPluginsToClassPath(pluginsFiles)
        val policyUtils = new PolicyUtils {

        }
        val fragmentUtils = new FragmentUtils {

        }
        val policy = fragmentUtils.getPolicyWithFragments(policyUtils.getPolicyById(policyId))
        val startingInfo = s"Starting policy in cluster"
        log.info(startingInfo)
        policyStatusUtils.updateStatus(PolicyStatusModel(id = policyId, status = Starting, statusInfo = Some(startingInfo)))
        val streamingContextService = StreamingContextService()
        val (spartaWorkflow, ssc) = streamingContextService.clusterStreamingContext(policy, pluginsFiles)
        policyStatusUtils.updateStatus(PolicyStatusModel(
          id = policyId,
          status = NotDefined,
          submissionId = Option(extractSparkApplicationId(ssc.sparkContext.applicationId))))
        spartaWorkflow.setup()
        ssc.start
        val policyConfigUtils = new PolicyConfigUtils {}
        val startedInfo = s"Started correctly application id: ${ssc.sparkContext.applicationId}"
        log.info(startedInfo)
        policyStatusUtils.updateStatus(PolicyStatusModel(
          id = policyId,
          status = Started,
          submissionId = Option(extractSparkApplicationId(ssc.sparkContext.applicationId)),
          statusInfo = Some(startedInfo),
          resourceManagerUrl = ResourceManagerLinkHelper.getLink(
            policyConfigUtils.executionMode(policy), policy.monitoringLink)
        ))
        ssc.awaitTermination()
        spartaWorkflow.cleanUp()
      } match {
        case Success(_) =>
          val information = s"Stopped correctly Kong Cloud cluster job"
          log.info(information)
          policyStatusUtils.updateStatus(PolicyStatusModel(id = policyId, status = Stopped, statusInfo = Some(information)))
        case Failure(exception) =>
          val information = s"Error initiating Kong Cloud cluster job"
          log.error(information)
          policyStatusUtils.updateStatus(PolicyStatusModel(
            id = policyId,
            status = Failed,
            statusInfo = Option(information),
            lastError = Option(PolicyErrorModel(information, PhaseEnum.Execution, exception.toString))
          ))
          throw DriverException(information, exception)
      }
    } match {
      case Success(_) =>
        log.info("Finished correctly Kong Cloud cluster job")
      case Failure(driverException: DriverException) =>
        log.error(driverException.msg, driverException.getCause)
        throw driverException
      case Failure(exception) =>
        log.error(s"Error initiating Sparta environment: ${exception.getLocalizedMessage}", exception)
        throw exception
    }
  }

  //scalastyle:on

  def initSpartaConfig(detailConfig: String, zKConfig: String, locationConfig: String, clusterConfig: String): Unit = {
    val configStr =
      s"${detailConfig.stripPrefix("{").stripSuffix("}")}" +
        s"\n${zKConfig.stripPrefix("{").stripSuffix("}")}" +
        s"\n${locationConfig.stripPrefix("{").stripSuffix("}")}" +
        s"\n${clusterConfig.stripPrefix("{").stripSuffix("}")}"
    log.info(s"Parsed config: sparta { $configStr }")
    KongCloudConfig.initMainConfig(Option(ConfigFactory.parseString(s"sparta{$configStr}")))
  }

  def extractSparkApplicationId(contextId: String): String = {
    if (contextId.contains("driver")) {
      val sparkApplicationId = contextId.substring(contextId.indexOf("driver"))
      log.info(s"The extracted Framework id is: ${contextId.substring(0, contextId.indexOf("driver") - 1)}")
      log.info(s"The extracted Spark application id is: $sparkApplicationId")
      sparkApplicationId
    } else contextId
  }
}
