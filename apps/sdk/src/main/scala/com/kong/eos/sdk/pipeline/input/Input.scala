
package com.kong.eos.sdk.pipeline.input

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.properties.{CustomProperties, Parameterizable}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * 抽像输入数据源类定义
  * @param properties
  */
abstract class Input(properties: Map[String, JSerializable]) extends Parameterizable(properties) with CustomProperties {

  val customKey = "inputOptions"
  val customPropertyKey = "inputOptionsKey"
  val customPropertyValue = "inputOptionsValue"
  val propertiesWithCustom = properties ++ getCustomProperties

  def setUp(options: Map[String, String] = Map.empty[String, String]): Unit = {}

  def cleanUp(options: Map[String, String] = Map.empty[String, String]): Unit = {}

  def initStream(ssc: StreamingContext, storageLevel: String): DStream[Row]

  def storageLevel(sparkStorageLevel: String): StorageLevel = {
    StorageLevel.fromString(sparkStorageLevel)
  }
}

object Input {

  val ClassSuffix = "Input"
  val SparkSubmitConfigurationMethod = "getSparkSubmitConfiguration"
  val RawDataKey = "_attachment_body"
  val InitSchema = Map(Input.RawDataKey -> StructType(Seq(StructField(Input.RawDataKey, StringType))))
}