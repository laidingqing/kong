

package com.kong.eos.sdk.pipeline.output

import java.io.{Serializable => JSerializable}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.sdk.properties.{CustomProperties, Parameterizable}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

/**
  * 抽像输出数据源定义类
  * @param name
  * @param properties
  */
abstract class Output(val name: String, properties: Map[String, JSerializable]) extends Parameterizable(properties) with SLF4JLogging with CustomProperties {

  val customKey = "saveOptions"
  val customPropertyKey = "saveOptionsKey"
  val customPropertyValue = "saveOptionsValue"
  val propertiesWithCustom = properties ++ getCustomProperties

  def setUp(options: Map[String, String] = Map.empty[String, String]): Unit = {}

  def cleanUp(options: Map[String, String] = Map.empty[String, String]): Unit = {}

  def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit

  def supportedSaveModes: Seq[SaveModeEnum.Value] = SaveModeEnum.allSaveModes

  def getProperties(): Map[String, JSerializable] = propertiesWithCustom

  def validateSaveMode(saveMode: SaveModeEnum.Value): Unit = {
    if (!supportedSaveModes.contains(saveMode))
      log.info(s"Save mode $saveMode selected not supported by the output $name." +
        s" Using the default mode ${SaveModeEnum.Append}"
      )
  }
}

object Output extends SLF4JLogging {

  final val ClassSuffix = "Output"
  final val SparkConfigurationMethod = "getSparkConfiguration"
  final val Separator = "_"
  final val FieldsSeparator = ","
  final val PrimaryKey = "primaryKey"
  final val TableNameKey = "tableName"
  final val PartitionByKey = "partitionBy"
  final val TimeDimensionKey = "timeDimension"
  final val MeasureMetadataKey = "measure"
  final val PrimaryKeyMetadataKey = "pk"

  def getSparkSaveMode(saveModeEnum: SaveModeEnum.Value): SaveMode =
    saveModeEnum match {
      case SaveModeEnum.Append => SaveMode.Append
      case SaveModeEnum.ErrorIfExists => SaveMode.ErrorIfExists
      case SaveModeEnum.Overwrite => SaveMode.Overwrite
      case SaveModeEnum.Ignore => SaveMode.Ignore
      case SaveModeEnum.Upsert => SaveMode.Append
      case _ =>
        log.warn(s"Save Mode $saveModeEnum not supported, using default save mode ${SaveModeEnum.Append}")
        SaveMode.Append
    }

  def getTimeFromOptions(options: Map[String, String]): Option[String] = options.get(TimeDimensionKey).notBlank

  def getPrimaryKeyOptions(options: Map[String, String]): Option[String] = options.get(PrimaryKey).notBlank

  def getTableNameFromOptions(options: Map[String, String]): String =
    options.getOrElse(TableNameKey, {
      log.error("Table name not defined")
      throw new NoSuchElementException("tableName not found in options")
    })

  def applyPartitionBy(options: Map[String, String],
                       dataFrame: DataFrameWriter[Row],
                       schemaFields: Array[StructField]): DataFrameWriter[Row] = {

    options.get(PartitionByKey).notBlank.fold(dataFrame)(partitions => {
      val fieldsInDataFrame = schemaFields.map(field => field.name)
      val partitionFields = partitions.split(",")
      if (partitionFields.forall(field => fieldsInDataFrame.contains(field)))
        dataFrame.partitionBy(partitionFields: _*)
      else {
        log.warn(s"Impossible to execute partition by fields: $partitionFields because the dataFrame not contain all" +
          s" fields. The dataFrame only contains: ${fieldsInDataFrame.mkString(",")}")
        dataFrame
      }
    })
  }

  def defaultTimeStampField(fieldName: String, nullable: Boolean, metadata: Metadata = Metadata.empty): StructField =
    StructField(fieldName, TimestampType, nullable, metadata)

  def defaultDateField(fieldName: String, nullable: Boolean, metadata: Metadata = Metadata.empty): StructField =
    StructField(fieldName, DateType, nullable, metadata)

  def defaultStringField(fieldName: String, nullable: Boolean, metadata: Metadata = Metadata.empty): StructField =
    StructField(fieldName, StringType, nullable, metadata)

  def defaultGeoField(fieldName: String, nullable: Boolean, metadata: Metadata = Metadata.empty): StructField =
    StructField(fieldName, ArrayType(DoubleType), nullable, metadata)

  def defaultLongField(fieldName: String, nullable: Boolean, metadata: Metadata = Metadata.empty): StructField =
    StructField(fieldName, LongType, nullable, metadata)
}