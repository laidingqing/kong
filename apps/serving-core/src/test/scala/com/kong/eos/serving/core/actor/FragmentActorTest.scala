
package com.kong.eos.serving.core.actor

import java.util

import akka.actor.{ActorSystem, Props}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import akka.util.Timeout
import com.kong.eos.serving.core.actor.FragmentActor.{Response, ResponseFragment, ResponseFragments}
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api._
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.data.Stat
import org.json4s.jackson.Serialization.read
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.util.Success

@RunWith(classOf[JUnitRunner])
class FragmentActorTest extends TestKit(ActorSystem("FragmentActorSpec"))
  with DefaultTimeout
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockitoSugar with KongCloudSerializer {

  trait TestData {

    val fragment =
      """
        |{
        |  "id": "id",
        |  "fragmentType": "input",
        |  "name": "inputname",
        |  "description": "input description",
        |  "shortDescription": "input description",
        |  "element": {
        |    "name": "input",
        |    "type": "input",
        |    "configuration": {
        |      "configKey": "configValue"
        |    }
        |  }
        |}
      """.stripMargin
    val otherFragment =
      """
        |{
        |  "id": "id2",
        |  "fragmentType": "input",
        |  "name": "inputname",
        |  "description": "input description",
        |  "shortDescription": "input description",
        |  "element": {
        |    "name": "input",
        |    "type": "input",
        |    "configuration": {
        |      "configKey": "configValue"
        |    }
        |  }
        |}
      """.stripMargin

    val fragmentElementModel = read[FragmentElementModel](fragment)
    val curatorFramework = mock[CuratorFramework]
    val getChildrenBuilder = mock[GetChildrenBuilder]
    val getDataBuilder = mock[GetDataBuilder]
    val existsBuilder = mock[ExistsBuilder]
    val createBuilder = mock[CreateBuilder]
    val deleteBuilder = mock[DeleteBuilder]
    val protectedACL = mock[ProtectACLCreateModeStatPathAndBytesable[String]]
    val setDataBuilder = mock[SetDataBuilder]
    implicit var oAuth2Info = OAuth2Info(0, "username", "*")
    val fragmentActor = system.actorOf(Props(new FragmentActor()))
    implicit val timeout: Timeout = Timeout(15.seconds)
  }

  override def afterAll: Unit = shutdown()

  "FragmentActor" must {

    "findByType: returns an empty Seq because the node of type not exists yet" in new TestData {
      when(curatorFramework.getChildren)
        .thenReturn(getChildrenBuilder)
      when(curatorFramework.getChildren
        .forPath("/stratio/sparta/fragments/input"))
        .thenThrow(new NoNodeException)

      fragmentActor ! FragmentActor.FindByType(null, "input")

      expectMsg(new ResponseFragments(Success(Seq())))
    }

    "findByTypeAndId: returns a failure holded by a ResponseFragment when the node does not exist" in new TestData {
      when(curatorFramework.getChildren)
        .thenReturn(getChildrenBuilder)
      when(curatorFramework.getChildren
        .forPath("/stratio/sparta/fragments/input"))
        .thenThrow(new NoNodeException)

      fragmentActor ! FragmentActor.FindByTypeAndId(null, "input", "id")

      expectMsgAnyClassOf(classOf[ResponseFragment])
    }

    "findByTypeAndName: returns a failure holded by a ResponseFragment when the node does not exist" in new TestData {
      when(curatorFramework.getChildren)
        .thenReturn(getChildrenBuilder)
      when(curatorFramework.getChildren
        .forPath("/stratio/sparta/fragments/input"))
        .thenThrow(new NoNodeException)

      fragmentActor ! FragmentActor.FindByTypeAndName(null, "input", "inputname")

      expectMsgAnyClassOf(classOf[ResponseFragment])
    }

    "findByTypeAndName: returns a failure holded by a ResponseFragment when no such element" in new TestData {
      when(curatorFramework.getChildren)
        .thenReturn(getChildrenBuilder)
      when(curatorFramework.getChildren
        .forPath("/stratio/sparta/fragments/input"))
        .thenThrow(new NoSuchElementException)

      fragmentActor ! FragmentActor.FindByTypeAndName(null, "input", "inputname")

      expectMsgAnyClassOf(classOf[ResponseFragment])
    }

    // XXX create
    "create: creates a fragment and return the created fragment" in new TestData {
      when(curatorFramework.checkExists())
        .thenReturn(existsBuilder)
      // scalastyle:off null
      when(curatorFramework.checkExists()
        .forPath("/stratio/sparta/fragments/input"))
        .thenReturn(null)
      // scalastyle:on null
      when(curatorFramework.getChildren)
        .thenReturn(getChildrenBuilder)
      when(curatorFramework.getChildren
        .forPath("/stratio/sparta/fragments/input"))
        .thenReturn(util.Arrays.asList("id"))
      when(curatorFramework.create)
        .thenReturn(createBuilder)
      when(curatorFramework.create
        .creatingParentsIfNeeded)
        .thenReturn(protectedACL)
      when(curatorFramework.create
        .creatingParentsIfNeeded
        .forPath("/stratio/sparta/fragments/input/element"))
        .thenReturn(fragment)

      fragmentActor ! FragmentActor.Create(fragmentElementModel)

      expectMsgAnyClassOf(classOf[ResponseFragment])
    }

    "create: tries to create a fragment but it is impossible because the fragment exists" in new TestData {
      when(curatorFramework.checkExists())
        .thenReturn(existsBuilder)
      when(curatorFramework.checkExists()
        .forPath("/stratio/sparta/fragments/input"))
        .thenReturn(new Stat())
      when(curatorFramework.getChildren)
        .thenReturn(getChildrenBuilder)
      when(curatorFramework.getChildren
        .forPath("/stratio/sparta/fragments/input"))
        .thenReturn(util.Arrays.asList("id"))

      fragmentActor ! FragmentActor.Create(fragmentElementModel)

      expectMsgAnyClassOf(classOf[ResponseFragment])
    }
  }

  // XXX update
  "update: updates a fragment" in new TestData {
    when(curatorFramework.checkExists())
      .thenReturn(existsBuilder)
    // scalastyle:off null
    when(curatorFramework.checkExists()
      .forPath("/stratio/sparta/fragments/input"))
      .thenReturn(null)
    when(curatorFramework.getChildren)
      .thenReturn(getChildrenBuilder)
    when(curatorFramework.getChildren
      .forPath("/stratio/sparta/fragments/input"))
      .thenReturn(util.Arrays.asList("id"))
    when(curatorFramework.setData)
      .thenReturn(setDataBuilder)
    when(curatorFramework.setData
      .forPath("/stratio/sparta/fragments/input/element"))
      .thenReturn(new Stat())

    fragmentActor ! FragmentActor.Update(null, fragmentElementModel)

    expectMsg(new Response(Success(fragmentElementModel)))
    // scalastyle:on null
  }

  "update: tries to update a fragment but it is impossible because the fragment exists" in new TestData {
    when(curatorFramework.checkExists())
      .thenReturn(existsBuilder)
    when(curatorFramework.checkExists()
      .forPath("/stratio/sparta/fragments/input"))
      .thenReturn(new Stat())
    when(curatorFramework.getChildren)
      .thenReturn(getChildrenBuilder)
    when(curatorFramework.getChildren
      .forPath("/stratio/sparta/fragments/input"))
      .thenReturn(util.Arrays.asList("id"))
    when(curatorFramework.getData)
      .thenReturn(getDataBuilder)
    when(curatorFramework.getData
      .forPath("/stratio/sparta/fragments/input/id"))
      .thenReturn(otherFragment.getBytes)

    fragmentActor ! FragmentActor.Update(null, fragmentElementModel)

    expectMsgAnyClassOf(classOf[FragmentActor.Response])
  }

  "update: tries to update a fragment but it is impossible because the fragment does not exist" in new TestData {
    when(curatorFramework.checkExists())
      .thenReturn(existsBuilder)
    when(curatorFramework.checkExists()
      .forPath("/stratio/sparta/fragments/input"))
      .thenReturn(new Stat())
    when(curatorFramework.getChildren)
      .thenReturn(getChildrenBuilder)
    when(curatorFramework.getChildren
      .forPath("/stratio/sparta/fragments/input"))
      .thenReturn(util.Arrays.asList("id"))
    when(curatorFramework.getData)
      .thenReturn(getDataBuilder)
    when(curatorFramework.getData
      .forPath("/stratio/sparta/fragments/input/id"))
      .thenReturn(fragment.getBytes)

    when(curatorFramework.setData)
      .thenReturn(setDataBuilder)
    when(curatorFramework.setData
      .forPath("/stratio/sparta/fragments/input/element"))
      .thenThrow(new NoNodeException)

    fragmentActor ! FragmentActor.Update(null, fragmentElementModel)

    expectMsgAnyClassOf(classOf[FragmentActor.Response])
  }

  // XXX deleteByTypeAndId
  "deleteByTypeAndId: deletes a fragment by its type and its id" in new TestData {
    // scalastyle:off null
    when(curatorFramework.delete)
      .thenReturn(deleteBuilder)
    when(curatorFramework.delete
      .forPath("/stratio/sparta/fragments/input/id"))
      .thenReturn(null)

    fragmentActor ! FragmentActor.DeleteByTypeAndId(null, "input", "id")

    expectMsg(new Response(Success()))
    // scalastyle:on null
  }

  "deleteByTypeAndId: deletes a fragment but it is impossible because the fragment does not exists" in new TestData {
    when(curatorFramework.delete)
      .thenReturn(deleteBuilder)
    when(curatorFramework.delete
      .forPath("/stratio/sparta/fragments/input/id"))
      .thenThrow(new NoNodeException)

    fragmentActor ! FragmentActor.DeleteByTypeAndId(null, "input", "id")

    expectMsgAnyClassOf(classOf[FragmentActor.Response])
  }
}
