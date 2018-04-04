
package com.kong.eos.serving.api.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.kong.eos.driver.service.StreamingContextService
import com.kong.eos.serving.core.actor.{RequestActor, FragmentActor, StatusActor}
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AkkaConstant
import org.apache.curator.framework.CuratorFramework
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ControllerActorTest(_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory {

  KongCloudConfig.initMainConfig()
  KongCloudConfig.initApiConfig()

  val curatorFramework = mock[CuratorFramework]
  val statusActor = _system.actorOf(Props(new StatusActor()))
  val executionActor = _system.actorOf(Props(new RequestActor()))
  val streamingContextService = new StreamingContextService()
  val fragmentActor = _system.actorOf(Props(new FragmentActor()))
  val policyActor = _system.actorOf(Props(new PolicyActor(statusActor)))
  val sparkStreamingContextActor = _system.actorOf(
    Props(new LauncherActor(streamingContextService)))
  val pluginActor = _system.actorOf(Props(new PluginActor()))
  val configActor = _system.actorOf(Props(new ConfigActor()))

  def this() =
    this(ActorSystem("ControllerActorSpec", KongCloudConfig.daemonicAkkaConfig))

  implicit val actors = Map(
    AkkaConstant.StatusActorName -> statusActor,
    AkkaConstant.FragmentActorName -> fragmentActor,
    AkkaConstant.PolicyActorName -> policyActor,
    AkkaConstant.LauncherActorName -> sparkStreamingContextActor,
    AkkaConstant.PluginActorName -> pluginActor,
    AkkaConstant.ExecutionActorName -> executionActor,
    AkkaConstant.ConfigActorName -> configActor
  )

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "ControllerActor" should {
    "set up the controller actor that contains all sparta's routes without any error" in {
      _system.actorOf(Props(new ControllerActor(actors)))
    }
  }
}
