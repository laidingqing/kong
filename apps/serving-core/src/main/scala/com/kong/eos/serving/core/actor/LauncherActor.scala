

package com.kong.eos.serving.core.actor

import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.models.submit.SubmitRequest


object LauncherActor {

  case class Launch(policy: PolicyModel)

  case class Start(policy: PolicyModel)

  case class StartWithRequest(policy: PolicyModel, request: SubmitRequest)
}
