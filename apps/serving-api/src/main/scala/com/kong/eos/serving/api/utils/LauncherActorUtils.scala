
package com.kong.eos.serving.api.utils

import akka.actor.{Props, _}
import com.kong.eos.driver.service.StreamingContextService
import com.kong.eos.serving.api.actor.{LocalLauncherActor, MarathonLauncherActor}
import com.kong.eos.serving.core.actor.ClusterLauncherActor
import com.kong.eos.serving.core.actor.LauncherActor.Start
import com.kong.eos.serving.core.constants.AkkaConstant._
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.utils.PolicyStatusUtils

trait LauncherActorUtils extends PolicyStatusUtils {

  val contextLauncherActorPrefix = "contextLauncherActor"

  val streamingContextService: StreamingContextService

  def launch(policy: PolicyModel, context: ActorContext): PolicyModel = {
    if (isAvailableToRun(policy)) {
      log.info("Streaming Context Available, launching policy ... ")
      val actorName = cleanActorName(s"$contextLauncherActorPrefix-${policy.name}")
      val policyActor = context.children.find(children => children.path.name == actorName)

      val launcherActor = policyActor match {
        case Some(actor) =>
          actor
        case None =>
          log.info(s"Launched -> $actorName")
          if (isExecutionType(policy, AppConstant.ConfigLocal)) {
            log.info(s"Launching policy: ${policy.name} with actor: $actorName in local mode")
            context.actorOf(Props(
              new LocalLauncherActor(streamingContextService)), actorName)
          } else {
            if(isExecutionType(policy, AppConstant.ConfigMarathon)) {
              log.info(s"Launching policy: ${policy.name} with actor: $actorName in marathon mode")
              context.actorOf(Props(new MarathonLauncherActor()), actorName)
            }
            else {
              log.info(s"Launching policy: ${policy.name} with actor: $actorName in cluster mode")
              context.actorOf(Props(new ClusterLauncherActor()), actorName)
            }
          }
      }
      launcherActor ! Start(policy)
    }
    policy
  }
}
