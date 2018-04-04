
package com.kong.eos.plugin.output.print

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._

import scala.util.Try

class PrintOutput(name: String, properties: Map[String, JSerializable])
  extends Output(name, properties) {

  val printData = Try(properties.getBoolean("printData")).getOrElse(false)
  val printSchema = Try(properties.getBoolean("printSchema")).getOrElse(false)
  val printMetadata = Try(properties.getBoolean("printMetadata")).getOrElse(true)
  val logLevel = properties.getString("logLevel", "warn")

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val metadataInfo = if(printMetadata)
      Option(s"Table name : ${Output.getTableNameFromOptions(options)}\tElements: ${dataFrame.count()}") else None
    val schemaInfo = if(printSchema) Option(s"DataFrame schema: ${dataFrame.schema.toString()}") else None

    logLevel match {
      case "warn" =>
        metadataInfo.foreach(info => log.warn(info))
        schemaInfo.foreach(schema => log.warn(schema))
        if(printData) dataFrame.foreach(row => log.warn(row.mkString(",")))
      case "error" =>
        metadataInfo.foreach(info => log.error(info))
        schemaInfo.foreach(schema => log.error(schema))
        if(printData) dataFrame.foreach(row => log.error(row.mkString(",")))
      case _ =>
        metadataInfo.foreach(info => log.error(info))
        schemaInfo.foreach(schema => log.info(schema))
        if(printData) dataFrame.foreach(row => log.info(row.mkString(",")))
    }
  }
}
