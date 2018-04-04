
package com.kong.eos.driver

import akka.actor.{ActorSystem, Props}
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AkkaConstant
import com.kong.eos.serving.core.utils.PluginsFilesUtils
import com.google.common.io.BaseEncoding
import com.kong.eos.driver.actor.MarathonAppActor
import com.kong.eos.driver.actor.MarathonAppActor.StartApp
import com.kong.eos.driver.exception.DriverException
import com.typesafe.config.ConfigFactory

import scala.util.{Failure, Success, Try}

object MarathonDriver extends PluginsFilesUtils {

  val NumberOfArguments = 3
  val PolicyIdIndex = 0
  val ZookeeperConfigurationIndex = 1
  val DetailConfigurationIndex = 2

  def main(args: Array[String]): Unit = {
    assert(args.length == NumberOfArguments,
      s"Invalid number of arguments: ${args.length}, args: $args, expected: $NumberOfArguments")
    Try {
      val policyId = args(PolicyIdIndex)
      val zookeeperConf = new String(BaseEncoding.base64().decode(args(ZookeeperConfigurationIndex)))
      val detailConf = new String(BaseEncoding.base64().decode(args(DetailConfigurationIndex)))
      initSpartaConfig(zookeeperConf, detailConf)
      val system = ActorSystem(policyId)
      val marathonAppActor =
        system.actorOf(Props(new MarathonAppActor()), AkkaConstant.MarathonAppActorName)

      marathonAppActor ! StartApp(policyId)
    } match {
      case Success(_) =>
        log.info("Initiated Marathon App environment")
      case Failure(driverException: DriverException) =>
        log.error(driverException.msg, driverException.getCause)
        throw driverException
      case Failure(exception) =>
        log.error(s"Error initiating Marathon App environment: ${exception.getLocalizedMessage}", exception)
        throw exception
    }
  }

  def initSpartaConfig(zKConfig: String, detailConf: String): Unit = {
    val configStr = s"${detailConf.stripPrefix("{").stripSuffix("}")}" +
      s"\n${zKConfig.stripPrefix("{").stripSuffix("}")}"
    log.info(s"Parsed config: sparta { $configStr }")
    val composedStr = s" sparta { $configStr } "
    KongCloudConfig.initMainWithFallbackConfig(ConfigFactory.parseString(composedStr))
  }
}
