
package com.kong.eos.serving.core.utils

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.constants.AppConstant._
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}

trait PolicyConfigUtils extends SLF4JLogging {

  val DetailConfig = KongCloudConfig.getDetailConfig.getOrElse {
    val message = "Impossible to extract Detail Configuration"
    log.error(message)
    throw new RuntimeException(message)
  }

  def isExecutionType(policy: PolicyModel, executionType: String): Boolean =
    policy.executionMode match {
      case Some(executionMode) if executionMode.nonEmpty => executionMode.equalsIgnoreCase(executionType)
      case _ => DetailConfig.getString(ExecutionMode).equalsIgnoreCase(executionType)
    }

  def isCluster(policy: PolicyModel, clusterConfig: Config): Boolean =
    policy.sparkConf.find(sparkProp =>
      sparkProp.sparkConfKey == DeployMode && sparkProp.sparkConfValue == ClusterValue) match {
      case Some(mode) => true
      case _ => Try(clusterConfig.getString(DeployMode)) match {
        case Success(mode) => mode == ClusterValue
        case Failure(e) => false
      }
    }

  def getDetailExecutionMode(policy: PolicyModel, clusterConfig: Config): String =
    if (isExecutionType(policy, AppConstant.ConfigLocal)) LocalValue
    else {
      val execMode = executionMode(policy)
      if (isCluster(policy, clusterConfig)) s"$execMode-$ClusterValue"
      else s"$execMode-$ClientValue"
    }

  def pluginsJars(policy: PolicyModel): Seq[String] =
    policy.userPluginsJars.map(userJar => userJar.jarPath.trim)

  def gracefulStop(policy: PolicyModel): Option[Boolean] = policy.stopGracefully

  def executionMode(policy: PolicyModel): String = policy.executionMode match {
    case Some(mode) if mode.nonEmpty => mode
    case _ => DetailConfig.getString(ExecutionMode)
  }
}
