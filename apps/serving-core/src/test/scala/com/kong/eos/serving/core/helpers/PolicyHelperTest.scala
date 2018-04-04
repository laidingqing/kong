
package com.kong.eos.serving.core.helpers

import com.kong.eos.serving.core.models.policy.{PolicyModel, UserJar}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PolicyHelperTest extends WordSpecLike with Matchers with MockitoSugar {
  val aggModel: PolicyModel = mock[PolicyModel]

  "PolicyHelper" should {
    "return files" in {
      when(aggModel.userPluginsJars).thenReturn(Seq(UserJar("path1"), UserJar("path2")))

      val files = PolicyHelper.jarsFromPolicy(aggModel)

      files.map(_.getName) shouldBe Seq("path1", "path2")
    }

    "return empty Seq" in {
      when(aggModel.userPluginsJars).thenReturn(Seq(UserJar("")))

      val files = PolicyHelper.jarsFromPolicy(aggModel)

      files.size shouldBe 0
    }
  }
}
