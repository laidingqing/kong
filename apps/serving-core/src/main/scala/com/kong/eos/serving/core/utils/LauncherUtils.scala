
package com.kong.eos.serving.core.utils

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.models.policy.PolicyStatusModel
import com.typesafe.config.Config

import scala.util.{Failure, Success, Try}

trait LauncherUtils extends SLF4JLogging{

  def loggingResponsePolicyStatus(response: Try[PolicyStatusModel]): Unit =
    response match {
      case Success(statusModel) =>
        log.info(s"Policy status model created or updated correctly: " +
          s"\n\tId: ${statusModel.id}\n\tStatus: ${statusModel.status}")
      case Failure(e) =>
        log.error(s"Policy status model creation failure. Error: ${e.getLocalizedMessage}", e)
    }

  def getZookeeperConfig: Config = KongCloudConfig.getZookeeperConfig.getOrElse {
    val message = "Impossible to extract Zookeeper Configuration"
    log.error(message)
    throw new RuntimeException(message)
  }
}
