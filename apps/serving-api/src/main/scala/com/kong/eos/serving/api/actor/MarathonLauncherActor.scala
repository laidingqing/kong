
package com.kong.eos.serving.api.actor

import akka.actor.{Actor, Cancellable, PoisonPill}
import com.kong.eos.serving.core.marathon.MarathonService
import com.kong.eos.serving.core.actor.LauncherActor.Start
import com.kong.eos.serving.core.actor.StatusActor.ResponseStatus
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant._
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum._
import com.kong.eos.serving.core.models.policy.{PhaseEnum, PolicyErrorModel, PolicyModel, PolicyStatusModel}
import com.kong.eos.serving.core.models.submit.SubmitRequest
import com.kong.eos.serving.core.services.ClusterCheckerService
import com.kong.eos.serving.core.utils._
import org.apache.curator.framework.CuratorFramework

import scala.util.{Failure, Success, Try}

class MarathonLauncherActor() extends Actor
  with LauncherUtils with SchedulerUtils with SparkSubmitUtils with ClusterListenerUtils with ArgumentsUtils
  with PolicyStatusUtils with RequestUtils {

  private val clusterCheckerService = new ClusterCheckerService()
  private val checkersPolicyStatus = scala.collection.mutable.ArrayBuffer.empty[Cancellable]

  override def receive: PartialFunction[Any, Unit] = {
    case Start(policy: PolicyModel) => initializeSubmitRequest(policy)
    case ResponseStatus(status) => loggingResponsePolicyStatus(status)
    case _ => log.info("Unrecognized message in Marathon Launcher Actor")
  }

  override def postStop(): Unit = checkersPolicyStatus.foreach(_.cancel())

  def initializeSubmitRequest(policy: PolicyModel): Unit = {
    Try {
      log.info(s"Initializing options for submit Marathon application associated to policy: ${policy.name}")
      val zookeeperConfig = getZookeeperConfig
      val clusterConfig = KongCloudConfig.getClusterConfig(Option(ConfigMesos)).get
      val master = clusterConfig.getString(Master).trim
      val driverFile = extractMarathonDriverSubmit(policy, DetailConfig, KongCloudConfig.getHdfsConfig)
      val pluginsFiles = pluginsJars(policy)
      val driverArguments =
        extractDriverArguments(policy, driverFile, clusterConfig, zookeeperConfig, ConfigMesos, pluginsFiles)
      val (sparkSubmitArguments, sparkConfigurations) =
        extractSubmitArgumentsAndSparkConf(policy, clusterConfig, pluginsFiles)
      val submitRequest = SubmitRequest(policy.id.get, SpartaDriverClass, driverFile, master, sparkSubmitArguments,
        sparkConfigurations, driverArguments, ConfigMesos, killUrl(clusterConfig))
      val detailExecMode = getDetailExecutionMode(policy, clusterConfig)

      createRequest(submitRequest).getOrElse(throw new Exception("Impossible to create submit request in persistence"))

      (new MarathonService(context, policy, submitRequest), detailExecMode)
    } match {
      case Failure(exception) =>
        val information = s"Error when initializing Kong Cloud Marathon App options"
        log.error(information, exception)
        updateStatus(PolicyStatusModel(id = policy.id.get, status = Failed, statusInfo = Option(information),
          lastError = Option(PolicyErrorModel(information, PhaseEnum.Execution, exception.toString))
        ))
        self ! PoisonPill
      case Success((marathonApp, detailExecMode)) =>
        val information = "Kong Cloud Marathon App configurations initialized correctly"
        log.info(information)
        updateStatus(PolicyStatusModel(id = policy.id.get, status = NotStarted,
          statusInfo = Option(information), lastExecutionMode = Option(detailExecMode)))
        marathonApp.launch(detailExecMode)
        addMarathonContextListener(policy.id.get, policy.name, context, Option(self))
        checkersPolicyStatus += scheduleOneTask(AwaitPolicyChangeStatus, DefaultAwaitPolicyChangeStatus)(
          clusterCheckerService.checkPolicyStatus(policy, self, context))
    }
  }
}