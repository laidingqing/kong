
package com.kong.eos.plugin.output.http

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, OutputFormatEnum, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._

import scala.util.Try
import scalaj.http._
/**
  * This output send all AggregateOperations or DataFrames information through a REST operation.
  *
  * @param name
  * @param properties
  */
class HttpOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties) {

  val MaxReadTimeout = 5000
  val MaxConnTimeout = 1000

  require(properties.getString("url", None).isDefined, "url must be provided")

  val url = properties.getString("url")
  val delimiter = properties.getString("delimiter", ",")
  val readTimeout = Try(properties.getInt("readTimeout")).getOrElse(MaxReadTimeout)
  val connTimeout = Try(properties.getInt("connTimeout")).getOrElse(MaxConnTimeout)
  val outputFormat = OutputFormatEnum.withName(properties.getString("outputFormat", "json").toUpperCase)
  val postType = PostType.withName(properties.getString("postType", "body").toUpperCase)
  val parameterName = properties.getString("parameterName", "")
  val contentType = if (outputFormat == OutputFormatEnum.ROW) "text/plain" else "application/json"

  override def supportedSaveModes: Seq[SaveModeEnum.Value] = Seq(SaveModeEnum.Append)

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {

    validateSaveMode(saveMode)
    executor(dataFrame)
  }

  private def executor(dataFrame: DataFrame): Unit = {
    outputFormat match {
      case OutputFormatEnum.ROW => dataFrame.rdd.foreachPartition(partition =>
        partition.foreach(row => sendData(row.mkString(delimiter))))
      case _ => dataFrame.toJSON.foreachPartition(partition =>
        partition.foreach(row => sendData(row)))
    }
  }

  def sendData(formattedData: String): HttpResponse[String] = {
    postType match {
      case PostType.PARAMETER => Http(url)
        .postForm(Seq(parameterName -> formattedData)).asString
      case PostType.BODY => Http(url).postData(formattedData)
        .header("content-type", contentType)
        .header("Charset", "UTF-8")
        .option(HttpOptions.readTimeout(readTimeout)).asString
    }
  }
}
