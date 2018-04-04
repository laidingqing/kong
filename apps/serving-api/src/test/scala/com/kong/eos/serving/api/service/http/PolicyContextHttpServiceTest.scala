package com.kong.eos.serving.api.service.http

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import com.kong.eos.sdk.exception.MockException
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.LauncherActor
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.actor.{FragmentActor, LauncherActor}
import com.kong.eos.serving.core.actor.FragmentActor.{Response, ResponseFragment}
import com.kong.eos.serving.core.actor.LauncherActor.Launch
import com.kong.eos.serving.core.actor.StatusActor.{FindAll, FindById, ResponseStatus, Update}
import com.kong.eos.serving.core.constants.AkkaConstant
import com.kong.eos.serving.core.models.dto.{LoggedUserConstant, OAuth2Constants}
import com.kong.eos.serving.core.models.policy.{PolicyStatusModel, ResponsePolicy}
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import spray.http.StatusCodes

import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class PolicyContextHttpServiceTest extends WordSpec
  with PolicyContextHttpService
  with HttpServiceBaseTest {

  val sparkStreamingTestProbe = TestProbe()
  val fragmentActorTestProbe = TestProbe()
  val statusActorTestProbe = TestProbe()
  val dummyUser = OAuth2Constants.AnonymousUser

  override implicit val actors: Map[String, ActorRef] = Map(
    AkkaConstant.LauncherActorName -> sparkStreamingTestProbe.ref,
    AkkaConstant.FragmentActorName -> fragmentActorTestProbe.ref,
    AkkaConstant.StatusActorName -> statusActorTestProbe.ref
  )

  override val supervisor: ActorRef = testProbe.ref

  "PolicyContextHttpService.findAll" should {
    "find all policy contexts" in {
      val statusActorAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FindAll =>
              sender ! Success(Seq(getPolicyStatusModel()))
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, statusActorTestProbe, statusActorAutoPilot)
      Get(s"/${HttpConstant.PolicyContextPath}") ~> routes(dummyUser) ~> check {
        statusActorTestProbe.expectMsg(FindAll)
        responseAs[Seq[PolicyStatusModel]] should equal(Seq(getPolicyStatusModel()))
      }
    }
    "return a 500 if there was any error" in {
      val statusActorAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FindAll =>
              sender ! Failure(new MockException)
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, statusActorTestProbe, statusActorAutoPilot)
      Get(s"/${HttpConstant.PolicyContextPath}") ~> routes(dummyUser) ~> check {
        statusActorTestProbe.expectMsg(FindAll)
        status should be(StatusCodes.InternalServerError)
      }
    }
  }

  "PolicyContextHttpService.find" should {
    "find policy contexts by id" in {
      val statusActorAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FindById(id) =>
              sender ! ResponseStatus(Success(getPolicyStatusModel()))
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, statusActorTestProbe, statusActorAutoPilot)
      Get(s"/${HttpConstant.PolicyContextPath}/id") ~> routes(dummyUser) ~> check {
        statusActorTestProbe.expectMsg(FindById("id"))
        responseAs[PolicyStatusModel] should equal(getPolicyStatusModel())
      }
    }
    "return a 500 if there was any error" in {
      val statusActorAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FindById(id) =>
              sender ! ResponseStatus(Failure(new MockException))
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, statusActorTestProbe, statusActorAutoPilot)
      Get(s"/${HttpConstant.PolicyContextPath}/id") ~> routes(dummyUser) ~> check {
        statusActorTestProbe.expectMsg(FindById("id"))
        status should be(StatusCodes.InternalServerError)
      }
    }
  }

  "PolicyContextHttpService.update" should {
    "update a policy context when the id of the contexts exists" in {
      val statusActorAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case Update(policyStatus) =>
              sender ! ResponseStatus(Try(policyStatus))
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, statusActorTestProbe, statusActorAutoPilot)
      Put(s"/${HttpConstant.PolicyContextPath}", getPolicyStatusModel()) ~> routes(dummyUser) ~> check {
        statusActorTestProbe.expectMsgType[Update]
        status should be(StatusCodes.Created)
      }
    }
    "return a 500 if there was any error" in {
      val statusActorAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case Update(policyStatus) =>
              sender ! ResponseStatus(Try(throw new Exception))
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, statusActorTestProbe, statusActorAutoPilot)
      Put(s"/${HttpConstant.PolicyContextPath}", getPolicyStatusModel()) ~> routes(dummyUser) ~> check {
        statusActorTestProbe.expectMsgType[Update]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }


  "PolicyContextHttpService.create" should {
    "creates a policy context when the id of the contexts exists" in {
      val fragmentAutopilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = {
          msg match {
            case FragmentActor.PolicyWithFragments(fragment) =>
              sender ! ResponsePolicy(Try(getPolicyModel()))
            case FragmentActor.Create(fragment) => sender ! None
          }
          TestActor.KeepRunning
        }
      })

      startAutopilot(None, fragmentActorTestProbe, fragmentAutopilot)
      startAutopilot(Success(getPolicyModel()))

      Post(s"/${HttpConstant.PolicyContextPath}", getPolicyModel()) ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[Launch]
        status should be(StatusCodes.OK)
      }

      fragmentAutopilot.foreach(_.noAutoPilot)
    }
    "return a 500 if there was any error" in {
      startAutopilot(Failure(new MockException))
      Post(s"/${HttpConstant.PolicyContextPath}", getPolicyModel()) ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[Launch]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }
}
