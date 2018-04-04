
package com.kong.eos.serving.core.config

import akka.event.slf4j.SLF4JLogging
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try

case class KongCloudConfigFactory(fromConfig: Option[Config]) extends SLF4JLogging {

  def getConfig(node: String, currentConfig: Option[Config] = None): Option[Config] =
    fromConfig match {
      case Some(fromCfg) => currentConfig match {
        case Some(config: Config) => Try(config.getConfig(node)).toOption
        case None => Some(fromCfg.getConfig(node))
      }
      case None => currentConfig match {
        case Some(config: Config) => Try(config.getConfig(node)).toOption
        case None => Try(ConfigFactory.load().getConfig(node)).toOption
      }
    }
}

object KongCloudConfigFactory {

  def apply(configOption: Config): KongCloudConfigFactory = new KongCloudConfigFactory(Option(configOption))

  def apply(): KongCloudConfigFactory = new KongCloudConfigFactory(None)
}


