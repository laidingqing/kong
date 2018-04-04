package com.kong.eos.serving.api.service.http

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import com.kong.eos.sdk.exception.MockException
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.PolicyActor
import com.kong.eos.serving.api.actor.PolicyActor.{Delete, FindByFragment, ResponsePolicies}
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.actor.FragmentActor
import com.kong.eos.serving.core.actor.FragmentActor._
import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.constants.AkkaConstant
import com.kong.eos.serving.core.models.dto.{LoggedUserConstant, OAuth2Constants}
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import spray.http.StatusCodes

import scala.util.{Failure, Success}

@RunWith(classOf[JUnitRunner])
class FragmentHttpServiceTest extends WordSpec
  with FragmentHttpService
  with HttpServiceBaseTest {

  val policyTestProbe = TestProbe()
  val fragmentTestProbe = TestProbe()
  val dummyUser = OAuth2Constants.AnonymousUser

  override implicit val actors: Map[String, ActorRef] = Map(
    AkkaConstant.PolicyActorName -> policyTestProbe.ref,
    AkkaConstant.FragmentActorName -> fragmentTestProbe.ref
  )
  override val supervisor: ActorRef = testProbe.ref

  override def beforeEach(): Unit = {
    KongCloudConfig.initMainConfig(Option(localConfig), KongCloudConfigFactory(localConfig))
  }

  "FragmentHttpService.findByTypeAndId" should {
    "find a fragment" in {
      startAutopilot(ResponseFragment(Success(getFragmentModel())))
      Get(s"/${HttpConstant.FragmentPath}/input/id/fragmentId") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[FindByTypeAndId]
        responseAs[FragmentElementModel] should equal(getFragmentModel())
      }
    }
    "return a 500 if there was any error" in {
      startAutopilot(ResponseFragment(Failure(new MockException())))
      Get(s"/${HttpConstant.FragmentPath}/input/id/fragmentId") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[FindByTypeAndId]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }

  "FragmentHttpService.findByTypeAndName" should {
    "find a fragment" in {
      startAutopilot(ResponseFragment(Success(getFragmentModel())))
      Get(s"/${HttpConstant.FragmentPath}/input/name/fragment") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[FindByTypeAndName]
        responseAs[FragmentElementModel] should equal(getFragmentModel())
      }
    }
    "return a 500 if there was any error" in {
      startAutopilot(ResponseFragment(Failure(new MockException())))
      Get(s"/${HttpConstant.FragmentPath}/input/name/fragment") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[FindByTypeAndName]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }

  "FragmentHttpService.findAllByType" should {
    "find all fragments" in {
      startAutopilot(ResponseFragments(Success(Seq(getFragmentModel()))))
      Get(s"/${HttpConstant.FragmentPath}/input") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[FindByType]
        responseAs[Seq[FragmentElementModel]] should equal(Seq(getFragmentModel()))
      }
    }
    "return a 500 if there was any error" in {
      startAutopilot(ResponseFragment(Failure(new MockException())))
      Get(s"/${HttpConstant.FragmentPath}/input") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[FindByType]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }

  "FragmentHttpService.create" should {
    "return the fragment that was created" in {
      startAutopilot(ResponseFragment(Success(getFragmentModel())))
      Post(s"/${HttpConstant.FragmentPath}", getFragmentModel) ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[Create]
        responseAs[FragmentElementModel] should equal(getFragmentModel())
      }
    }
    "return a 500 if there was any error" in {
      startAutopilot(Response(Failure(new MockException())))
      Post(s"/${HttpConstant.FragmentPath}", getFragmentModel) ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[Create]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }

  "FragmentHttpService.deleteByTypeAndId" should {
    "return an OK because the fragment was deleted" in {
      val policyAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FindByFragment(input, id) =>
              sender ! ResponsePolicies(Success(Seq(getPolicyModel())))
              TestActor.NoAutoPilot
            case Delete =>
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, policyTestProbe, policyAutoPilot)
      startAutopilot(Response(Success(None)))
      Delete(s"/${HttpConstant.FragmentPath}/input/id/fragmentId") ~> routes(dummyUser) ~> check {
        testProbe.expectMsgType[DeleteByTypeAndId]
        policyTestProbe.expectMsgType[FindByFragment]
        policyTestProbe.expectMsgType[Delete]
        status should be(StatusCodes.OK)
      }
    }
  }

  "FragmentHttpService.update" should {
    /*"return an OK because the fragment was updated" in {
      val policy = getPolicyModel().copy(fragments = Seq(getFragmentModel()))
      val fragmentAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FragmentActor.Update(input) =>
              sender ! Response(Success(getFragmentModel()))
              TestActor.NoAutoPilot
          }
      })
      val policyAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case PolicyActor.FindAll() =>
              sender ! ResponsePolicies(Success(Seq(policy)))
              TestActor.NoAutoPilot
            case PolicyActor.Update(policy) =>
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, fragmentTestProbe, fragmentAutoPilot)
      startAutopilot(None, policyTestProbe, policyAutoPilot)
      Put(s"/${HttpConstant.FragmentPath}", getFragmentModel(Some("id"))) ~> routes ~> check {
        fragmentTestProbe.expectMsgType[Update]
        policyTestProbe.expectMsgType[PolicyActor.FindAll]
        policyTestProbe.expectMsgType[PolicyActor.Update]
        status should be(StatusCodes.OK)
      }
    }*/

    "return a 500 if there was any error" in {
      val fragmentAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case FragmentActor.Update(null, input) =>
              sender ! Response(Failure(new MockException()))
              TestActor.NoAutoPilot
          }
      })
      val policyAutoPilot = Option(new TestActor.AutoPilot {
        def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
          msg match {
            case PolicyActor.FindAll() =>
              sender ! ResponsePolicies(Success(Seq(getPolicyModel())))
              TestActor.NoAutoPilot
            case PolicyActor.Update(policy) =>
              TestActor.NoAutoPilot
          }
      })
      startAutopilot(None, fragmentTestProbe, fragmentAutoPilot)
      startAutopilot(None, policyTestProbe, policyAutoPilot)

      Put(s"/${HttpConstant.FragmentPath}", getFragmentModel(Some("id"))) ~> routes(dummyUser) ~> check {
        fragmentTestProbe.expectMsgType[Update]
        status should be(StatusCodes.InternalServerError)
      }
    }
  }
}
