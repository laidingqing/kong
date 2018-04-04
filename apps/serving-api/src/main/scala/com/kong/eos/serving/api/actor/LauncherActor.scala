
package com.kong.eos.serving.api.actor

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import com.kong.eos.serving.api.utils.LauncherActorUtils
import com.kong.eos.driver.service.StreamingContextService
import com.kong.eos.serving.api.utils.LauncherActorUtils
import com.kong.eos.serving.core.actor.LauncherActor.Launch
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.utils.PolicyUtils
import com.kong.eos.serving.api.utils.LauncherActorUtils
import org.apache.curator.framework.CuratorFramework

import scala.util.Try

class LauncherActor(val streamingContextService: StreamingContextService) extends Actor with LauncherActorUtils with PolicyUtils {

  override val supervisorStrategy: OneForOneStrategy =
    OneForOneStrategy() {
      case _: ServingCoreException => Escalate
      case t =>
        super.supervisorStrategy.decider.applyOrElse(t, (_: Any) => Escalate)
    }

  override def receive: Receive = {
    case Launch(policy) => sender ! create(policy)
    case _ => log.info("Unrecognized message in Launcher Actor")
  }

  def create(policy: PolicyModel): Try[PolicyModel] =
    Try {
      if (policy.id.isEmpty) createPolicy(policy)
      launch(policy, context)
    }
}