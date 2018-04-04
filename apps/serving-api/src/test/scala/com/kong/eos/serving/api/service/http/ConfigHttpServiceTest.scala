
package com.kong.eos.serving.api.service.http

import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.ConfigActor
import com.kong.eos.serving.api.actor.ConfigActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.constants.{AkkaConstant, AppConstant}
import com.kong.eos.serving.core.models.dto.{LoggedUserConstant, OAuth2Constants}
import com.kong.eos.serving.core.models.frontend.FrontendConfiguration
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigHttpServiceTest extends WordSpec
  with ConfigHttpService
  with HttpServiceBaseTest{

  val configActorTestProbe = TestProbe()

  val dummyUser = OAuth2Constants.AnonymousUser

  override implicit val actors: Map[String, ActorRef] = Map(
    AkkaConstant.ConfigActorName -> configActorTestProbe.ref
  )

  override val supervisor: ActorRef = testProbe.ref

  override def beforeEach(): Unit = {
    KongCloudConfig.initMainConfig(Option(localConfig), KongCloudConfigFactory(localConfig))
  }

  protected def retrieveStringConfig(): FrontendConfiguration =
    FrontendConfiguration(AppConstant.DefaultFrontEndTimeout, Option(AppConstant.DefaultOauth2CookieName))

  "ConfigHttpService.FindAll" should {
    "retrieve a FrontendConfiguration item" in {
      startAutopilot(ConfigResponse(retrieveStringConfig()))
      Get(s"/${HttpConstant.ConfigPath}") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[ConfigActor.FindAll.type]
        responseAs[FrontendConfiguration] should equal(retrieveStringConfig())
      }
    }
  }

}
