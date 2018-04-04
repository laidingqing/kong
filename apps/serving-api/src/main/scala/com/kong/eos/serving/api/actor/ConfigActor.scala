
package com.kong.eos.serving.api.actor

import akka.actor.{Actor, _}
import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.ConfigActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.frontend.FrontendConfiguration
import com.kong.eos.serving.api.constants.HttpConstant
import com.typesafe.config.Config
import spray.httpx.Json4sJacksonSupport

import scala.util.Try

class ConfigActor extends Actor with SLF4JLogging with Json4sJacksonSupport with KongCloudSerializer {

  val apiPath = HttpConstant.ConfigPath

  val oauthConfig: Option[Config] = KongCloudConfig.getOauth2Config
  val enabledSecurity: Boolean = Try(oauthConfig.get.getString("enable").toBoolean).getOrElse(false)
  val cookieName: String = Try(oauthConfig.get.getString("cookieName")).getOrElse(AppConstant.DefaultOauth2CookieName)

  override def receive: Receive = {
    case FindAll => findFrontendConfig()
    case _ => log.info("Unrecognized message in ConfigActor")
  }

  def findFrontendConfig(): Unit = {
    sender ! ConfigResponse(retrieveStringConfig())
  }

  def retrieveStringConfig(): FrontendConfiguration = {
    enabledSecurity match {
      case true => FrontendConfiguration(
        Try(KongCloudConfig.getFrontendConfig.get
          .getInt("timeout")).getOrElse(AppConstant.DefaultFrontEndTimeout), Option(cookieName))
      case false => FrontendConfiguration(Try(KongCloudConfig.getFrontendConfig.get
        .getInt("timeout")).getOrElse(AppConstant.DefaultFrontEndTimeout), Option(""))
    }
  }

}

object ConfigActor {
  object FindAll
  case class ConfigResponse(frontendConfiguration:FrontendConfiguration)
}
