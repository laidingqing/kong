
package com.kong.eos.serving.core

import akka.event.slf4j.SLF4JLogging

import scala.util.{Failure, Success, Try}

/**
 * This wrapper around Java's system is necessary because we need to mock it.
 */
trait System extends SLF4JLogging {

  def getenv(env: String): Option[String] = {
    Try(System.getenv(env)) match {
      case Success(environment) => Option(environment)
      case Failure(exception) => {
        log.debug(exception.getLocalizedMessage, exception)
        None
      }
    }
  }

  def getProperty(key: String, defaultValue: String): Option[String] = {
    Try(System.getProperty(key, defaultValue)) match {
      case Success(property) => Option(property)
      case Failure(exception) => {
        log.debug(exception.getLocalizedMessage, exception)
        None
      }
    }
  }
}

/**
 * Sparta's system wrapper used in SpartaHelper.
 */
class SpartaSystem extends System {}

/**
 * Sparta's system wrapper used as a mock in SpartaHelperSpec.
 * @param env that contains mocked environment variables.
 * @param properties that contains mocked properties.
 */
case class MockSystem(env: Map[String, String], properties: Map[String, String]) extends System {

  override def getenv(env: String): Option[String] = {
    this.env.get(env)
  }

  override def getProperty(key: String, defaultValue: String): Option[String] = {
    this.properties.get(key).orElse(Some(defaultValue))
  }
}
