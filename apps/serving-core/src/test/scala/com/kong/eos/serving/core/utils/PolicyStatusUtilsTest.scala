
package com.kong.eos.serving.core.utils

import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PolicyStatusUtilsTest extends BaseUtilsTest with PolicyStatusUtils {

  "SparkStreamingContextActor.isAnyPolicyStarted" should {

    "return false if there is no policy Starting/Started mode" in {
      KongCloudConfig.initMainConfig(Option(yarnConfig), KongCloudConfigFactory(yarnConfig))

      val response = isAnyPolicyStarted
      response should be(false)
    }
  }

  "SparkStreamingContextActor.isContextAvailable" should {
    "return true when execution mode is yarn" in {
      val response = isAvailableToRun(getPolicyModel())
      response should be(true)
    }

    "return false when execution mode is mesos" in {
      KongCloudConfig.initMainConfig(Option(mesosConfig), KongCloudConfigFactory(mesosConfig))

      val response = isAnyPolicyStarted
      response should be(false)
    }


    "return true when execution mode is local and there is no running policy" in {
      KongCloudConfig.initMainConfig(Option(localConfig), KongCloudConfigFactory(localConfig))

      val response = isAvailableToRun(getPolicyModel())
      response should be(true)
    }

    "return true when execution mode is standalone and there is no running policy" in {
      KongCloudConfig.initMainConfig(Option(standaloneConfig), KongCloudConfigFactory(standaloneConfig))

      val response = isAvailableToRun(getPolicyModel())
      response should be(true)
    }
  }
}
