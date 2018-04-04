
package com.kong.eos.serving.core.utils

import com.google.common.io.BaseEncoding
import com.typesafe.config.{Config, ConfigRenderOptions}

trait ArgumentsUtils {

  def render(config: Config, key: String): String = config.atKey(key).root.render(ConfigRenderOptions.concise)

  def encode(value: String): String = BaseEncoding.base64().encode(value.getBytes)

  def keyConfigEncoded(key: String, config: Config): String = encode(render(config, key))

  def keyOptionConfigEncoded(key: String, opConfig: Option[Config]): String =
    opConfig match {
      case Some(config) => keyConfigEncoded(key, config)
      case None => encode(" ")
    }

  def pluginsEncoded(plugins: Seq[String]): String = encode((Seq(" ") ++ plugins).mkString(","))

}
