package com.kong.eos.sdk.pipeline.transformation

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import org.apache.spark.sql.DataFrame

class OutputMock(name: String, properties: Map[String, JSerializable])
  extends Output(name, properties) {

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {}
}
