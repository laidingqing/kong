
package com.kong.eos.driver.step

import java.io._

import com.kong.eos.driver.writer.WriterOptions

case class Trigger(name: String,
                   sql: String,
                   overLast: Option[String] = None,
                   computeEvery: Option[String] = None,
                   writerOptions: WriterOptions,
                   configuration: Map[String, Serializable])