
package com.kong.eos.plugin.output.parquet

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._
/**
 * This output save as parquet file the information.
 *
 * @param name
 * @param properties
 */
class ParquetOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties) {

  val path = properties.getString("path", None).notBlank
  require(path.isDefined, "Destination path is required. You have to set 'path' on properties")

  override def supportedSaveModes : Seq[SaveModeEnum.Value] =
    Seq(SaveModeEnum.Append, SaveModeEnum.ErrorIfExists, SaveModeEnum.Ignore, SaveModeEnum.Overwrite)

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)

    validateSaveMode(saveMode)

    val dataFrameWriter = dataFrame.write
      .options(getCustomProperties)
      .mode(getSparkSaveMode(saveMode))

    applyPartitionBy(options, dataFrameWriter, dataFrame.schema.fields).parquet(s"${path.get}/$tableName")
  }
}
