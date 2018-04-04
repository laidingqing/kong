
package com.kong.eos.serving.api.service.http

import akka.actor.ActorRef
import akka.event.slf4j.SLF4JLogging
import akka.util.Timeout
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.constants.AkkaConstant
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.dto.LoggedUser
import spray.httpx.Json4sJacksonSupport
import spray.routing._

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

/**
 * It gives common operations such as error handling, i18n, etc. All HttpServices should extend of it.
 */
trait BaseHttpService extends HttpService with Json4sJacksonSupport with SLF4JLogging with KongCloudSerializer {

  implicit val timeout: Timeout = Timeout(AkkaConstant.DefaultTimeout.seconds)

  implicit def executionContext: ExecutionContextExecutor = actorRefFactory.dispatcher

  implicit val actors: Map[String, ActorRef]

  val supervisor: ActorRef

  def routes(user: OAuth2Info): Route

}
