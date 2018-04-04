
package com.kong.eos.plugin.output.fileSystem

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, OutputFormatEnum, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
 * This output sends all AggregateOperations or DataFrames data  to a directory stored in
 * HDFS, Amazon S3, Azure, etc.
 *
 * @param name
 * @param properties
 */
class FileSystemOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties) {

  val FieldName = "extractedData"
  val path = properties.getString("path", None).notBlank
  require(path.isDefined, "Destination path is required. You have to set 'path' on properties")
  val outputFormat = OutputFormatEnum.withName(properties.getString("outputFormat", "json").toUpperCase)
  val delimiter = properties.getString("delimiter", ",")

  override def supportedSaveModes: Seq[SaveModeEnum.Value] =
    Seq(SaveModeEnum.Append, SaveModeEnum.ErrorIfExists, SaveModeEnum.Ignore, SaveModeEnum.Overwrite)

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)

    validateSaveMode(saveMode)

    if (outputFormat == OutputFormatEnum.JSON) {
      val dataFrameWriter = dataFrame.write.mode(getSparkSaveMode(saveMode))
      applyPartitionBy(options, dataFrameWriter, dataFrame.schema.fields).json(s"${path.get}/$tableName")
    }
    else {
      val colSeq = dataFrame.schema.fields.flatMap(field => Some(col(field.name))).toSeq
      val df = dataFrame.withColumn(FieldName, concat_ws(delimiter, colSeq: _*)).select(FieldName)
      df.write.text(s"${path.get}/$tableName")
    }
  }
}

