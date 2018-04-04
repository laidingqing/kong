
package com.kong.eos.serving.api.helpers

import com.kong.eos.serving.core.config.{KongCloudConfig, KongCloudConfigFactory}
import com.kong.eos.serving.core.models.files.{KongCloudFile, KongFilesResponse}
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalamock.scalatest._
import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 * Tests over KongCloud helper operations used to wake up a KongCloud's context.
 *
 */
@RunWith(classOf[JUnitRunner])
class KongCloudAppClusterLauncherActorTest extends FlatSpec with MockFactory with ShouldMatchers with Matchers {

  it should "init SpartaConfig from a file with a configuration" in {
    val config = ConfigFactory.parseString(
      """
        |sparta {
        | testKey : "testValue"
        |}
      """.stripMargin)

    val spartaConfig = KongCloudConfig.initConfig(node = "sparta", configFactory = KongCloudConfigFactory(config))
    spartaConfig.get.getString("testKey") should be("testValue")
  }

  it should "init a config from a given config" in {
    val config = ConfigFactory.parseString(
      """
        |sparta {
        |  testNode {
        |    testKey : "testValue"
        |  }
        |}
      """.stripMargin)

    val spartaConfig = KongCloudConfig.initConfig(node = "sparta", configFactory = KongCloudConfigFactory(config))
    val testNodeConfig = KongCloudConfig.initConfig("testNode", spartaConfig, KongCloudConfigFactory(config))
    testNodeConfig.get.getString("testKey") should be("testValue")
  }
}
