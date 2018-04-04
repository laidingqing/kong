
package com.kong.eos.serving.api.service.handler

import akka.actor.ActorSystem
import com.kong.eos.sdk.exception.MockException
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import spray.http.StatusCodes
import spray.httpx.Json4sJacksonSupport
import spray.routing.{Directives, HttpService, StandardRoute}
import spray.testkit.ScalatestRouteTest
import com.kong.eos.serving.api.service.handler.CustomExceptionHandler._
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.{ErrorModel, KongCloudSerializer}

@RunWith(classOf[JUnitRunner])
class CustomExceptionHandlerTest extends WordSpec
with Directives with ScalatestRouteTest with Matchers
with Json4sJacksonSupport with HttpService with KongCloudSerializer {

  def actorRefFactory: ActorSystem = system

  trait MyTestRoute {

    val exception: Throwable
    val route: StandardRoute = complete(throw exception)
  }

  def route(throwable: Throwable): StandardRoute = complete(throw throwable)

  "CustomExceptionHandler" should {
    "encapsulate a unknow error in an error model and response with a 500 code" in new MyTestRoute {
      val exception = new MockException
      Get() ~> sealRoute(route) ~> check {
        status should be(StatusCodes.InternalServerError)
        response.entity.asString should be(ErrorModel.toString(new ErrorModel("666", "unknown")))
      }
    }
    "encapsulate a serving api error in an error model and response with a 400 code" in new MyTestRoute {
      val exception = ServingCoreException.create(ErrorModel.toString(new ErrorModel("333", "testing exception")))
      Get() ~> sealRoute(route) ~> check {
        status should be(StatusCodes.NotFound)
        response.entity.asString should be(ErrorModel.toString(new ErrorModel("333", "testing exception")))
      }
    }
  }
}
