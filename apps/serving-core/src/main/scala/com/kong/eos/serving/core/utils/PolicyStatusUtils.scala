
package com.kong.eos.serving.core.utils

import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum
import com.kong.eos.serving.core.models.policy.{PolicyModel, PolicyStatusModel}
import com.kong.eos.serving.core.models.{ErrorModel, KongCloudSerializer}
import com.kong.eos.serving.core.storage.{MongoClientUtils, StorageFactoryHolder}
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.json4s.jackson.Serialization._

import scala.collection.JavaConversions
import scala.util.{Failure, Success, Try}

trait PolicyStatusUtils extends KongCloudSerializer with PolicyConfigUtils with MongoClientUtils{

  /** Functions used inside the StatusActor **/

  def clearLastError(id: String): Try[Option[PolicyStatusModel]] = {
    Try {
      val statusPath = s"${AppConstant.ContextPath}/$id"
//      if (Option(curatorFramework.checkExists.forPath(statusPath)).isDefined) {
//        val actualStatus = read[PolicyStatusModel](new String(curatorFramework.getData.forPath(statusPath)))
//        val newStatus = actualStatus.copy(lastError = None)
//        log.info(s"Clearing last error for context: ${actualStatus.id}")
//        curatorFramework.setData().forPath(statusPath, write(newStatus).getBytes)
//        Some(newStatus)
//      } else None
      None
    }
  }

  //scalastyle:off
  def updateStatus(policyStatus: PolicyStatusModel): Try[PolicyStatusModel] = {
    Try {
      val statusPath = s"${AppConstant.ContextPath}/${policyStatus.id}"
//      if (Option(curatorFramework.checkExists.forPath(statusPath)).isDefined) {
//        val actualStatus = read[PolicyStatusModel](new String(curatorFramework.getData.forPath(statusPath)))
//        val newStatus = policyStatus.copy(
//          status = if (policyStatus.status == PolicyStatusEnum.NotDefined) actualStatus.status
//          else policyStatus.status,
//          name = if (policyStatus.name.isEmpty) actualStatus.name
//          else policyStatus.name,
//          description = if (policyStatus.description.isEmpty) actualStatus.description
//          else policyStatus.description,
//          lastError = if (policyStatus.lastError.isDefined) policyStatus.lastError
//          else if (policyStatus.status == PolicyStatusEnum.NotStarted) None else actualStatus.lastError,
//          submissionId = if (policyStatus.submissionId.isDefined) policyStatus.submissionId
//          else if (policyStatus.status == PolicyStatusEnum.NotStarted) None else actualStatus.submissionId,
//          marathonId = if (policyStatus.marathonId.isDefined) policyStatus.marathonId
//          else if (policyStatus.status == PolicyStatusEnum.NotStarted) None else actualStatus.marathonId,
//          submissionStatus = if (policyStatus.submissionStatus.isEmpty) actualStatus.submissionStatus
//          else policyStatus.submissionStatus,
//          statusInfo = if (policyStatus.statusInfo.isEmpty) actualStatus.statusInfo
//          else policyStatus.statusInfo,
//          lastExecutionMode = if (policyStatus.lastExecutionMode.isEmpty) actualStatus.lastExecutionMode
//          else policyStatus.lastExecutionMode,
//          resourceManagerUrl = if (policyStatus.status == PolicyStatusEnum.Started) policyStatus.resourceManagerUrl
//          else if (policyStatus.status == PolicyStatusEnum.NotDefined) actualStatus.resourceManagerUrl else None
//        )
//        log.info(s"Updating context ${newStatus.id} with name ${newStatus.name.getOrElse("undefined")}:" +
//          s"\n\tStatus:\t${actualStatus.status}\t--->\t${newStatus.status}" +
//          s"\n\tStatus Information:\t${actualStatus.statusInfo.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.statusInfo.getOrElse("undefined")} " +
//          s"\n\tSubmission Id:\t${actualStatus.submissionId.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.submissionId.getOrElse("undefined")}" +
//          s"\n\tSubmission Status:\t${actualStatus.submissionStatus.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.submissionStatus.getOrElse("undefined")}" +
//          s"\n\tMarathon Id:\t${actualStatus.marathonId.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.marathonId.getOrElse("undefined")}" +
//          s"\n\tLast Error:\t${actualStatus.lastError.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.lastError.getOrElse("undefined")}" +
//          s"\n\tLast Execution Mode:\t${actualStatus.lastExecutionMode.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.lastExecutionMode.getOrElse("undefined")}" +
//          s"\n\tResource Manager URL:\t${actualStatus.resourceManagerUrl.getOrElse("undefined")}" +
//          s"\t--->\t${newStatus.resourceManagerUrl.getOrElse("undefined")}")
//        curatorFramework.setData().forPath(statusPath, write(newStatus).getBytes)
//        newStatus
//      } else createStatus(policyStatus)
//        .getOrElse(throw new ServingCoreException(
//          ErrorModel.toString(new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId,
//            s"Is not possible to create policy context with id ${policyStatus.id}."))))
      createStatus(policyStatus)
              .getOrElse(throw new ServingCoreException(
                ErrorModel.toString(new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId,
                  s"Is not possible to create policy context with id ${policyStatus.id}."))))
    }
  }

  //scalastyle:on

  def createStatus(policyStatus: PolicyStatusModel): Try[PolicyStatusModel] = {
    val statusPath = s"${AppConstant.ContextPath}/${policyStatus.id}"
//    if (CuratorFactoryHolder.existsPath(statusPath)) {
//      updateStatus(policyStatus)
//    } else {
//      Try {
//        log.info(s"Creating policy context ${policyStatus.id} to <${policyStatus.status}>")
//        curatorFramework.create.creatingParentsIfNeeded.forPath(statusPath, write(policyStatus).getBytes)
//        policyStatus
//      }
//    }
    updateStatus(policyStatus)
  }

  def findAllStatuses(): Try[Seq[PolicyStatusModel]] =
    Try {
//      val contextPath = s"${AppConstant.ContextPath}"
//      if (CuratorFactoryHolder.existsPath(contextPath)) {
//        val children = curatorFramework.getChildren.forPath(contextPath)
//        val policiesStatus = JavaConversions.asScalaBuffer(children).toList.map(element =>
//          read[PolicyStatusModel](new String(
//            curatorFramework.getData.forPath(s"${AppConstant.ContextPath}/$element")
//          ))
//        )
//        policiesStatus
//      } else

        Seq.empty[PolicyStatusModel]
    }

  def findStatusById(id: String): Try[PolicyStatusModel] =
    Try {
      val statusPath = s"${AppConstant.ContextPath}/$id"
//      if (Option(curatorFramework.checkExists.forPath(statusPath)).isDefined)
//        read[PolicyStatusModel](new String(curatorFramework.getData.forPath(statusPath)))
//      else
        throw new ServingCoreException(
        ErrorModel.toString(new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No policy context with id $id.")))
    }

  def deleteAllStatuses(): Try[_] =
    Try {
      val contextPath = s"${AppConstant.ContextPath}"

//      if (CuratorFactoryHolder.existsPath(contextPath)) {
//        val children = curatorFramework.getChildren.forPath(contextPath)
//        val policiesStatus = JavaConversions.asScalaBuffer(children).toList.map(element =>
//          read[PolicyStatusModel](new String(curatorFramework.getData.forPath(s"${AppConstant.ContextPath}/$element")))
//        )
//
//        policiesStatus.foreach(policyStatus => {
//          val statusPath = s"${AppConstant.ContextPath}/${policyStatus.id}"
//          if (Option(curatorFramework.checkExists.forPath(statusPath)).isDefined) {
//            log.info(s"Deleting context ${policyStatus.id} >")
//            curatorFramework.delete().forPath(statusPath)
//          } else throw new ServingCoreException(ErrorModel.toString(
//            new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No policy context with id ${policyStatus.id}.")))
//        })
//      }
      new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No policy context with id.")
    }

  def deleteStatus(id: String): Try[_] =
    Try {
//      val statusPath = s"${AppConstant.ContextPath}/$id"
//      if (Option(curatorFramework.checkExists.forPath(statusPath)).isDefined) {
//        log.info(s">> Deleting context $id >")
//        curatorFramework.delete().forPath(statusPath)
//      } else
        throw new ServingCoreException(ErrorModel.toString(
        new ErrorModel(ErrorModel.CodeNotExistsPolicyWithId, s"No policy context with id $id.")))
    }

  /**
   * Adds a listener to one policy and executes the callback when it changed.
   *
   * @param id       of the policy.
   * @param callback with a function that will be executed.
   */
  def addListener(id: String, callback: (PolicyStatusModel, NodeCache) => Unit): Unit = {
    val contextPath = s"${AppConstant.ContextPath}/$id"
//    val nodeCache: NodeCache = new NodeCache(curatorFramework, contextPath)
//    nodeCache.getListenable.addListener(new NodeCacheListener {
//      override def nodeChanged(): Unit = {
//        Try(new String(nodeCache.getCurrentData.getData)) match {
//          case Success(value) =>
//            callback(read[PolicyStatusModel](value), nodeCache)
//          case Failure(e) =>
//            log.error(s"NodeCache value: ${nodeCache.getCurrentData}", e)
//        }
//      }
//    })
//    nodeCache.start()
  }

  /** Functions used out of StatusActor **/

  def isAnyPolicyStarted: Boolean =
    findAllStatuses() match {
      case Success(statuses) =>
        statuses.exists(_.status == PolicyStatusEnum.Started) ||
          statuses.exists(_.status == PolicyStatusEnum.Starting) ||
          statuses.exists(_.status == PolicyStatusEnum.Launched)
      case Failure(e) =>
        log.error("Error when find all policy statuses.", e)
        false
    }

  def isAvailableToRun(policyModel: PolicyModel): Boolean =
    (isExecutionType(policyModel, AppConstant.ConfigLocal), isAnyPolicyStarted) match {
      case (false, _) =>
        true
      case (true, false) =>
        true
      case (true, true) =>
        log.warn(s"One policy is already launched")
        false
    }
}
