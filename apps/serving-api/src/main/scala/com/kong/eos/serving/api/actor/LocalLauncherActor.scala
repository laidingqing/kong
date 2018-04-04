
package com.kong.eos.serving.api.actor

import akka.actor.{Actor, PoisonPill}
import com.kong.eos.driver.factory.SparkContextFactory
import com.kong.eos.driver.service.StreamingContextService
import com.kong.eos.serving.core.actor.LauncherActor.Start
import com.kong.eos.serving.core.actor.StatusActor.ResponseStatus
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.helpers.{JarsHelper, PolicyHelper, ResourceManagerLinkHelper}
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum
import com.kong.eos.serving.core.models.policy.{PhaseEnum, PolicyErrorModel, PolicyModel, PolicyStatusModel}
import com.kong.eos.serving.core.utils.{LauncherUtils, PolicyConfigUtils, PolicyStatusUtils}
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.streaming.StreamingContext

import scala.util.{Failure, Success, Try}

class LocalLauncherActor(streamingContextService: StreamingContextService)
  extends Actor with PolicyConfigUtils with LauncherUtils with PolicyStatusUtils{

  override def receive: PartialFunction[Any, Unit] = {
    case Start(policy: PolicyModel) => doInitSpartaContext(policy)
    case ResponseStatus(status) => loggingResponsePolicyStatus(status)
    case _ => log.info("Unrecognized message in Local Launcher Actor")
  }

  private def doInitSpartaContext(policy: PolicyModel): Unit = {
    val jars = PolicyHelper.jarsFromPolicy(policy)

    jars.foreach(file => JarsHelper.addToClasspath(file))
    Try {
      val startingInfo = s"正在开始本地云计算任务"
      log.info(startingInfo)
      updateStatus(PolicyStatusModel(
        id = policy.id.get,
        status = PolicyStatusEnum.NotStarted,
        statusInfo = Some(startingInfo),
        lastExecutionMode = Option(AppConstant.LocalValue)
      ))
      val (spartaWorkflow, ssc) = streamingContextService.localStreamingContext(policy, jars)
      spartaWorkflow.setup()
      ssc.start()
      val startedInformation = s"本地云计算任务已正确"
      log.info(startedInformation)
      updateStatus(PolicyStatusModel(
        id = policy.id.get,
        status = PolicyStatusEnum.Started,
        statusInfo = Some(startedInformation),
        resourceManagerUrl = ResourceManagerLinkHelper.getLink(executionMode(policy), policy.monitoringLink)
      ))
      ssc.awaitTermination()
      spartaWorkflow.cleanUp()
    } match {
      case Success(_) =>
        val information = s"本地云计算任务已停止"
        log.info(information)
        updateStatus(PolicyStatusModel(
          id = policy.id.get, status = PolicyStatusEnum.Stopped, statusInfo = Some(information)))
        self ! PoisonPill
      case Failure(exception) =>
        val information = s"错误初始化本地云计算任务"
        log.error(information, exception)
        updateStatus(PolicyStatusModel(
          id = policy.id.get,
          status = PolicyStatusEnum.Failed,
          statusInfo = Option(information),
          lastError = Option(PolicyErrorModel(information, PhaseEnum.Execution, exception.toString))
        ))
        SparkContextFactory.destroySparkContext()
        self ! PoisonPill
    }
  }
}
