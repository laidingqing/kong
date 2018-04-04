
package com.kong.eos.serving.core.utils

import java.io.File
import java.util.Calendar

import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.constants.AppConstant._
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.storage.{MongoClientUtils, StorageFactoryHolder}
import org.apache.commons.io.FileUtils

import scala.util.{Failure, Success, Try}

trait CheckpointUtils extends PolicyConfigUtils with MongoClientUtils{

  /* PUBLIC METHODS */

  def deleteFromLocal(policy: PolicyModel): Unit = {
    val checkpointDirectory = checkpointPath(policy, checkTime = false)
    log.info(s"Deleting checkpoint directory: $checkpointDirectory")
    FileUtils.deleteDirectory(new File(checkpointDirectory))
  }

  def deleteFromHDFS(policy: PolicyModel): Unit = {
    val checkpointDirectory = checkpointPath(policy, checkTime = false)
    log.info(s"Deleting checkpoint directory: $checkpointDirectory")
    HdfsUtils().delete(checkpointDirectory)
  }

  def isHadoopEnvironmentDefined: Boolean =
    Option(System.getenv(SystemHadoopConfDir)) match {
      case Some(_) => true
      case None => false
    }

  def deleteCheckpointPath(policy: PolicyModel): Unit =
    Try {
      if (isExecutionType(policy, AppConstant.ConfigLocal)) deleteFromLocal(policy)
      else deleteFromHDFS(policy)
    } match {
      case Success(_) => log.info(s"Checkpoint deleted in folder: ${checkpointPath(policy, checkTime = false)}")
      case Failure(ex) => log.error("Cannot delete checkpoint folder", ex)
    }

  def createLocalCheckpointPath(policy: PolicyModel): Unit = {
    if (isExecutionType(policy, AppConstant.ConfigLocal))
      Try {
        createFromLocal(policy)
      } match {
        case Success(_) => log.info(s"Checkpoint created in folder: ${checkpointPath(policy, checkTime = false)}")
        case Failure(ex) => log.error("Cannot create checkpoint folder", ex)
      }
  }

  def checkpointPath(policy: PolicyModel, checkTime: Boolean = true): String = {
    val path = policy.checkpointPath.map(path => cleanCheckpointPath(path))
      .getOrElse(checkpointPathFromProperties(policy))

    if(checkTime && addTimeToCheckpointPath(policy))
      s"$path/${policy.name}/${Calendar.getInstance().getTime.getTime}"
    else s"$path/${policy.name}"
  }

  def autoDeleteCheckpointPath(policy: PolicyModel): Boolean =
    policy.autoDeleteCheckpoint.getOrElse(autoDeleteCheckpointPathFromProperties())


  /* PRIVATE METHODS */

  private def cleanCheckpointPath(path: String): String = {
    val hdfsPrefix = "hdfs://"

    if (path.startsWith(hdfsPrefix))
      log.info(s"The path starts with $hdfsPrefix and is not valid, it is replaced with empty value")
    path.replace(hdfsPrefix, "")
  }

  private def checkpointPathFromProperties(policy: PolicyModel): String =
    (for {
      config <- KongCloudConfig.getDetailConfig
      checkpointPath <- Try(cleanCheckpointPath(config.getString(ConfigCheckpointPath))).toOption
    } yield checkpointPath).getOrElse(generateDefaultCheckpointPath(policy))

  private def autoDeleteCheckpointPathFromProperties(): Boolean =
    Try(KongCloudConfig.getDetailConfig.get.getBoolean(ConfigAutoDeleteCheckpoint))
      .getOrElse(DefaultAutoDeleteCheckpoint)

  private def addTimeToCheckpointPath(policy: PolicyModel): Boolean =
    policy.addTimeToCheckpointPath.getOrElse(addTimeToCheckpointPathFromProperties())

  private def addTimeToCheckpointPathFromProperties(): Boolean =
    Try(KongCloudConfig.getDetailConfig.get.getBoolean(ConfigAddTimeToCheckpointPath))
      .getOrElse(DefaultAddTimeToCheckpointPath)

  private def generateDefaultCheckpointPath(policy: PolicyModel): String =
    executionMode(policy) match {
      case mode if mode == ConfigMesos =>
        DefaultCheckpointPathClusterMode +
          Try(KongCloudConfig.getHdfsConfig.get.getString(HadoopUserName))
            .getOrElse(DefaultHdfsUser) +
          DefaultHdfsUser
      case ConfigLocal =>
        DefaultCheckpointPathLocalMode
      case _ =>
        throw new RuntimeException("Error getting execution mode")
    }

  private def createFromLocal(policy: PolicyModel): Unit = {
    val checkpointDirectory = checkpointPath(policy)
    log.info(s"Creating checkpoint directory: $checkpointDirectory")
    FileUtils.forceMkdir(new File(checkpointDirectory))
  }
}
