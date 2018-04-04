
package com.kong.eos.serving.core.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit._
import akka.util.Timeout
import com.kong.eos.serving.core.actor.StatusActor.{ResponseDelete, ResponseStatus}
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum
import com.kong.eos.serving.core.models.policy.PolicyStatusModel
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api._
import org.apache.zookeeper.data.Stat
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.concurrent.duration._
import scala.util.Success

@RunWith(classOf[JUnitRunner])
class StatusActorTest extends TestKit(ActorSystem("FragmentActorSpec", KongCloudConfig.daemonicAkkaConfig))
  with WordSpecLike
  with Matchers
  with ImplicitSender
  with MockitoSugar {

  val curatorFramework = mock[CuratorFramework]
  val getChildrenBuilder = mock[GetChildrenBuilder]
  val getDataBuilder = mock[GetDataBuilder]
  val existsBuilder = mock[ExistsBuilder]
  val createBuilder = mock[CreateBuilder]
  val deleteBuilder = mock[DeleteBuilder]

  KongCloudConfig.initMainConfig()

  val actor = system.actorOf(Props(new StatusActor()))
  implicit val timeout: Timeout = Timeout(15.seconds)
  val id = "existingID"
  val status = new PolicyStatusModel("existingID", PolicyStatusEnum.Launched)
  val statusRaw =
    """
      |{
      |  "id": "existingID",
      |  "status": "Launched"
      |}
    """.stripMargin

  "statusActor" must {

    "find: returns success when find an existing ID " in {
      when(curatorFramework
        .checkExists())
        .thenReturn(existsBuilder)
      when(curatorFramework.checkExists()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenReturn(new Stat)
      // scalastyle:off null

      when(curatorFramework.getData())
        .thenReturn(getDataBuilder)
      when(curatorFramework.getData()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenReturn(statusRaw.getBytes)

      actor ! StatusActor.FindById(id)

      expectMsg(ResponseStatus(Success(status)))
      // scalastyle:on null

    }

    "delete: returns success when deleting an existing ID " in {
      when(curatorFramework
        .checkExists())
        .thenReturn(existsBuilder)
      when(curatorFramework.checkExists()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenReturn(new Stat)
      // scalastyle:off null

      when(curatorFramework.delete())
        .thenReturn(deleteBuilder)
      when(curatorFramework.delete()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenReturn(null)

      actor ! StatusActor.Delete(id)

      expectMsg(ResponseDelete(Success(null)))
      // scalastyle:on null

    }

    "delete: returns failure when deleting an unexisting ID " in {
      // scalastyle:off null
      when(curatorFramework
        .checkExists())
        .thenReturn(existsBuilder)
      when(curatorFramework.checkExists()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenReturn(null)

     actor ! StatusActor.Delete(id)

      expectMsgAnyClassOf(classOf[ResponseDelete])
      // scalastyle:on null

    }

    "delete: returns failure when deleting an existing ID and an error occurs while deleting" in {
      // scalastyle:off null
      when(curatorFramework
        .checkExists())
        .thenReturn(existsBuilder)
      when(curatorFramework.checkExists()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenReturn(new Stat())

      when(curatorFramework.delete())
        .thenReturn(deleteBuilder)
      when(curatorFramework.delete()
        .forPath(s"${AppConstant.ContextPath}/$id"))
        .thenThrow(new RuntimeException())
      actor ! StatusActor.Delete(id)

      expectMsgAnyClassOf(classOf[ResponseDelete])
      // scalastyle:on null

    }
  }
}
