
package com.kong.eos.serving.core.config

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.constants.AppConstant._
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Success, Try}

/**
  * Helper with common operations used to create a KongCloud context used to run the application.
  */
object KongCloudConfig extends SLF4JLogging {

  var mainConfig: Option[Config] = None
  var apiConfig: Option[Config] = None
  var oauth2Config: Option[Config] = None

  def initConfig(node: String,
                 currentConfig: Option[Config] = None,
                 configFactory: KongCloudConfigFactory = KongCloudConfigFactory()): Option[Config] = {
    log.info(s"Loading $node configuration")
    val configResult = currentConfig match {
      case Some(config) => Try(config.getConfig(node)).toOption
      case None => configFactory.getConfig(node)
    }
    assert(configResult.isDefined, s"Fatal Error: configuration can not be loaded: $node")
    configResult
  }

  def initWithFallbackConfig(node: String, currentConfig: Config): Option[Config] = {
    log.info(s"Loading $node configuration")
    val mergedConfig = ConfigFactory.load().withFallback(currentConfig)
    val configResult = Try(mergedConfig.getConfig(node)).toOption
    assert(configResult.isDefined, s"Fatal Error: configuration can not be loaded: $node")
    configResult
  }

  def initMainConfig(currentConfig: Option[Config] = None,
                     configFactory: KongCloudConfigFactory = KongCloudConfigFactory()): Option[Config] = {
    mainConfig = initConfig(ConfigAppName, currentConfig, configFactory)
    mainConfig
  }

  def initMainWithFallbackConfig(currentConfig: Config): Option[Config] = {
    mainConfig = initWithFallbackConfig(ConfigAppName, currentConfig)
    mainConfig
  }

  def initApiConfig(configFactory: KongCloudConfigFactory = KongCloudConfigFactory()): Option[Config] = {
    apiConfig = initConfig(ConfigApi, mainConfig, configFactory)
    apiConfig
  }

  def initOauth2Config(configFactory: KongCloudConfigFactory = KongCloudConfigFactory()): Option[Config] = {
    oauth2Config = initConfig(ConfigOauth2)
    oauth2Config
  }

  def getClusterConfig(executionMode: Option[String] = None): Option[Config] =
    Try {
      executionMode match {
        case Some(execMode) if execMode.nonEmpty => execMode
        case _ => getDetailConfig.get.getString(ExecutionMode)
      }
    } match {
      case Success(execMode) if execMode != ConfigLocal =>
        getOptionConfig(execMode, mainConfig.get)
      case _ =>
        log.error("Error when extracting cluster configuration")
        None
    }

  def getHdfsConfig: Option[Config] = mainConfig.flatMap(config => getOptionConfig(ConfigHdfs, config))

  def getDetailConfig: Option[Config] = mainConfig.flatMap(config => getOptionConfig(ConfigDetail, config))

  def getZookeeperConfig: Option[Config] = mainConfig.flatMap(config => getOptionConfig(ConfigZookeeper, config))

  def getFrontendConfig: Option[Config] = mainConfig.flatMap(config => getOptionConfig(ConfigFrontend, config))

  def getMongoConfig: Option[Config] = mainConfig.flatMap(config => getOptionConfig(ConfigMongo, config))

  def getOauth2Config: Option[Config] = oauth2Config match {
    case Some(config) => Some(config)
    case None => {
      oauth2Config = initOauth2Config()
      oauth2Config
    }
  }

  def getSprayConfig: Option[Config] = KongCloudConfigFactory().getConfig(ConfigSpray)

  def initOptionalConfig(node: String,
                         currentConfig: Option[Config] = None,
                         configFactory: KongCloudConfigFactory = KongCloudConfigFactory()): Option[Config] = {
    log.info(s" Loading $node configuration")
    Try(
      currentConfig match {
        case Some(config) => getOptionConfig(node, config)
        case None => configFactory.getConfig(node)
      }
    ).getOrElse(None)
  }

  def getOptionConfig(node: String, currentConfig: Config): Option[Config] =
    Try(currentConfig.getConfig(node)).toOption

  def getOptionStringConfig(node: String, currentConfig: Config): Option[String] =
    Try(currentConfig.getString(node)).toOption

  def daemonicAkkaConfig: Config = mainConfig match {
    case Some(mainSpartaConfig) =>
      mainSpartaConfig.withFallback(ConfigFactory.load(ConfigFactory.parseString("akka.daemonic=on")))
    case None => ConfigFactory.load(ConfigFactory.parseString("akka.daemonic=on"))
  }

}
