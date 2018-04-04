
package com.kong.eos.plugin.output.avro

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._
import com.databricks.spark.avro._
/**
  * This output save as avro file the information.
  */
class AvroOutput(name: String, properties: Map[String, Serializable]) extends Output(name, properties) {

  val path = properties.getString("path", None).notBlank
  require(path.isDefined, "Destination path is required. You have to set 'path' on properties")

  override def supportedSaveModes: Seq[SaveModeEnum.Value] =
    Seq(SaveModeEnum.Append, SaveModeEnum.ErrorIfExists, SaveModeEnum.Ignore, SaveModeEnum.Overwrite)

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)

    validateSaveMode(saveMode)

    val dataFrameWriter = dataFrame.write
      .options(getCustomProperties)
      .mode(getSparkSaveMode(saveMode))

    applyPartitionBy(options, dataFrameWriter, dataFrame.schema.fields).avro(s"${path.get}/$tableName")
  }
}
