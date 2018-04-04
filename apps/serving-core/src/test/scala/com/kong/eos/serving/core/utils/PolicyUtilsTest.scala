
package com.kong.eos.serving.core.utils

import com.kong.eos.serving.core.models.policy.PolicyModel
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PolicyUtilsTest extends BaseUtilsTest with PolicyUtils {

  val utils = spy(this)
  val basePath = "/samplePath"
  val aggModel: PolicyModel = mock[PolicyModel]

  "PolicyUtils.policyWithId" should {
    "return a policy with random UUID when there is no set id yet" in {
      val policy: PolicyModel = utils.policyWithId(getPolicyModel(None))
      policy.id shouldNot be(None)
    }

    "return a policy with the same id when there is set id" in {
      val policy: PolicyModel = utils.policyWithId(getPolicyModel(name = "TEST"))
      policy.id should be(Some("id"))
      policy.name should be("test")
    }
  }

  "PolicyUtils.populatePolicyWithRandomUUID" should {
    "return a policy copy with random UUID" in {
      utils.populatePolicyWithRandomUUID(getPolicyModel(id = None)).id shouldNot be(None)
    }
  }

  "PolicyUtils.existsByNameId" should {
    "return an existing policy with \"existingId\" from zookeeper" in {
      doReturn(true)
        .when(utils)
        .existsPath
      doReturn(Seq(
        getPolicyModel(id = Some("existingID")),
        getPolicyModel(id = Some("id#2")),
        getPolicyModel(id = Some("id#3"))))
        .when(utils)
        .findAllPolicies(withFragments = false)
      utils.existsPolicyByNameId(name = "myName", id = Some("existingID")).get should be(
        getPolicyModel(id = Some("existingID")))
    }

    "return an existing policy with not defined id but existing name from zookeeper" in {
      doReturn(true)
        .when(utils)
        .existsPath
      doReturn(Seq(
        getPolicyModel(id = Some("id#1"), name = "myname"),
        getPolicyModel(id = Some("id#2")),
        getPolicyModel(id = Some("id#3"))))
        .when(utils)
        .findAllPolicies(withFragments = false)

      val actualPolicy: PolicyModel = utils.existsPolicyByNameId(name = "MYNAME", id = None).get

      actualPolicy.name should be("myname")
      actualPolicy.id.get should be("id#1")
    }

    "return none when there is no policy with neither id or name" in {
      doReturn(true)
        .when(utils)
        .existsPath
      doReturn(Seq(
        getPolicyModel(id = Some("id#1"), name = "myname"),
        getPolicyModel(id = Some("id#2")),
        getPolicyModel(id = Some("id#3"))))
        .when(utils)
        .findAllPolicies(withFragments = true)

      utils.existsPolicyByNameId(name = "noName", id = None) should be(None)
    }

    "return none when there is some error or exception" in {
      doReturn(true)
        .when(utils)
        .existsPath
      doThrow(new RuntimeException)
        .when(utils)
        .findAllPolicies(withFragments = true)

      utils.existsPolicyByNameId(name = "noName", id = None) should be(None)
    }

    "return none when path not does not exists" in {
      doReturn(false)
        .when(utils)
        .existsPath

      utils.existsPolicyByNameId(name = "noName", id = None) should be(None)
    }
  }
}
