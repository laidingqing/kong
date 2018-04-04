
package com.kong.eos.serving.core.services

import akka.actor.{ActorContext, ActorRef}
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum._
import com.kong.eos.serving.core.models.policy.{PolicyModel, PolicyStatusModel}
import com.kong.eos.serving.core.utils.PolicyStatusUtils
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework

import scala.util.{Failure, Success}

class ClusterCheckerService() extends PolicyStatusUtils {

  def checkPolicyStatus(policy: PolicyModel, launcherActor: ActorRef, akkaContext: ActorContext): Unit = {
    findStatusById(policy.id.get) match {
      case Success(policyStatus) =>
        if (policyStatus.status == Launched || policyStatus.status == Starting || policyStatus.status == Uploaded ||
          policyStatus.status == Stopping || policyStatus.status == NotStarted) {
          val information = s"The checker detects that the policy not start/stop correctly"
          log.error(information)
          updateStatus(PolicyStatusModel(id = policy.id.get, status = Failed, statusInfo = Some(information)))
          akkaContext.stop(launcherActor)
        } else {
          val information = s"The checker detects that the policy start/stop correctly"
          log.info(information)
          updateStatus(PolicyStatusModel(id = policy.id.get, status = NotDefined, statusInfo = Some(information)))
        }
      case Failure(exception) =>
        log.error(s"Error when extract policy status in scheduler task.", exception)
    }
  }
}
