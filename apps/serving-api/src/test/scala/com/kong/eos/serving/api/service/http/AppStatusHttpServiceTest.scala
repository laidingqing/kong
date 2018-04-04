
package com.kong.eos.serving.api.service.http

import akka.actor.ActorRef
import com.kong.eos.serving.api.constants.HttpConstant
import org.apache.curator.framework.CuratorFramework
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import spray.http.StatusCodes

@RunWith(classOf[JUnitRunner])
class AppStatusHttpServiceTest extends WordSpec
                              with AppStatusHttpService
                              with HttpServiceBaseTest
with MockFactory {

  override implicit val actors: Map[String, ActorRef] = Map()
  override val supervisor: ActorRef = testProbe.ref

  "AppStatusHttpService" should {
    "check the status of the server" in {
      Get(s"/${HttpConstant.AppStatus}") ~> routes(null) ~> check {
        status should be (StatusCodes.InternalServerError)
      }
    }
  }
}


