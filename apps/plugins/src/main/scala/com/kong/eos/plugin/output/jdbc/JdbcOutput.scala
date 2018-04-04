
package com.kong.eos.plugin.output.jdbc

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.pipeline.output.SaveModeEnum.CloudSaveMode
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.SpartaJdbcUtils
import org.apache.spark.sql.jdbc.SpartaJdbcUtils._

import scala.util.{Failure, Success, Try}

class JdbcOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties) {

  require(properties.getString("url", None).isDefined, "url must be provided")

  val url = properties.getString("url")

  override def supportedSaveModes : Seq[CloudSaveMode] =
    Seq(SaveModeEnum.Append, SaveModeEnum.ErrorIfExists, SaveModeEnum.Ignore, SaveModeEnum.Overwrite)

  //scalastyle:off
  override def save(dataFrame: DataFrame, saveMode: CloudSaveMode, options: Map[String, String]): Unit = {
    validateSaveMode(saveMode)
    val tableName = getTableNameFromOptions(options)
    val sparkSaveMode = getSparkSaveMode(saveMode)
    val connectionProperties = new JDBCOptions(url,
      tableName,
      propertiesWithCustom.mapValues(_.toString).filter(_._2.nonEmpty)
    )

    Try {
      if (sparkSaveMode == SaveMode.Overwrite) SpartaJdbcUtils.dropTable(url, connectionProperties, tableName)

      SpartaJdbcUtils.tableExists(url, connectionProperties, tableName, dataFrame.schema)
    } match {
      case Success(tableExists) =>
        if (tableExists) {
          if (saveMode == SaveModeEnum.Upsert) {
            val updateFields = getPrimaryKeyOptions(options) match {
              case Some(pk) => pk.split(",").toSeq
              case None => dataFrame.schema.fields.filter(stField =>
                stField.metadata.contains(Output.PrimaryKeyMetadataKey)).map(_.name).toSeq
            }
            SpartaJdbcUtils.upsertTable(dataFrame, url, tableName, connectionProperties, updateFields)
          }

          if (saveMode == SaveModeEnum.Ignore) return

          if (saveMode == SaveModeEnum.ErrorIfExists) sys.error(s"Table $tableName already exists.")

          if (saveMode == SaveModeEnum.Append || saveMode == SaveModeEnum.Overwrite)
            SpartaJdbcUtils.saveTable(dataFrame, url, tableName, connectionProperties)
        } else log.warn(s"Table not created in Postgres: $tableName")
      case Failure(e) =>
        closeConnection()
        log.error(s"Error creating/dropping table $tableName")
    }
  }

  override def cleanUp(options: Map[String, String]): Unit = {
    log.info(s"Closing connections in JDBC Output: $name")
    closeConnection()
  }
}