package com.kong.eos.serving.api.service.http

import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.PluginActor.{PluginResponse, UploadPlugins}
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.models.dto.{LoggedUserConstant, OAuth2Constants}
import com.kong.eos.serving.core.models.files.{KongCloudFile, KongFilesResponse}
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import spray.http._

import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class PluginsHttpServiceTest extends WordSpec
  with PluginsHttpService
  with HttpServiceBaseTest {

  override val supervisor: ActorRef = testProbe.ref

  val pluginTestProbe = TestProbe()

  val dummyUser = OAuth2Constants.AnonymousUser

  override implicit val actors: Map[String, ActorRef] = Map.empty

  override def beforeEach(): Unit = {
    KongCloudConfig.initMainConfig(Option(localConfig), KongCloudConfigFactory(localConfig))
  }

  "PluginsHttpService.upload" should {
    "Upload a file" in {
      val response = KongFilesResponse(Success(Seq(KongCloudFile("", "", "", ""))))
      startAutopilot(response)
      Put(s"/${HttpConstant.PluginsPath}") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[UploadPlugins]
        status should be(StatusCodes.OK)
      }
    }
    "Fail when service is not available" in {
      val response = KongFilesResponse(Failure(new IllegalArgumentException("Error")))
      startAutopilot(response)
      Put(s"/${HttpConstant.PluginsPath}") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[UploadPlugins]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }
}
