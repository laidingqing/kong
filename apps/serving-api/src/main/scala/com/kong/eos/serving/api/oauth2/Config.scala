package com.kong.eos.serving.api.oauth2

import com.typesafe.config.ConfigFactory

import scala.util.Try

class Config(conf: com.typesafe.config.Config = ConfigFactory.load().getConfig("oauth2")) {
  val Enabled: Boolean = getDefaultBoolean("enable", false)
  val CheckTokenUrl: String = getDefaultString("url.check.token", Option(""))
  val CookieName: String = getDefaultString("cookieName", Option("user"))

  private def getDefaultBoolean(key: String, defaultValue: Boolean): Boolean = {
    Try(conf.getString(key).toBoolean).toOption match {
      case Some(x) => x
      case None => defaultValue
    }
  }

  private def getDefaultString(key: String, defaultValue: Option[String]): String = {
    val optionValue = Try(conf.getString(key)).toOption match {
      case Some(x) => Option(x)
      case None => defaultValue
    }
    optionValue.fold(throw new RuntimeException(s"$key is not defned"))(x => x)


  }

}