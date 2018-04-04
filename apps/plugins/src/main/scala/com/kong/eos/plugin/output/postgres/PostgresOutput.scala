
package com.kong.eos.plugin.output.postgres

import java.io.{InputStream, Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.pipeline.output.SaveModeEnum.CloudSaveMode
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.SpartaJdbcUtils
import org.apache.spark.sql.jdbc.SpartaJdbcUtils._
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection

import scala.util.{Failure, Success, Try}

class PostgresOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties) {

  require(properties.getString("url", None).isDefined, "Postgres url must be provided")

  val url = properties.getString("url")
  val bufferSize = properties.getString("bufferSize", "65536").toInt
  val delimiter = properties.getString("delimiter", "\t")
  val newLineSubstitution = properties.getString("newLineSubstitution", " ")
  val encoding = properties.getString("encoding", "UTF8")

  override def supportedSaveModes: Seq[CloudSaveMode] =
    Seq(SaveModeEnum.Append, SaveModeEnum.Overwrite, SaveModeEnum.Upsert)

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
        if (tableExists)
          if (saveMode == SaveModeEnum.Upsert) {
            val updateFields = getPrimaryKeyOptions(options) match {
              case Some(pk) => pk.split(",").toSeq
              case None => dataFrame.schema.fields.filter(stField =>
                stField.metadata.contains(Output.PrimaryKeyMetadataKey)).map(_.name).toSeq
            }
            SpartaJdbcUtils.upsertTable(dataFrame, url, tableName, connectionProperties, updateFields)
          } else {
            dataFrame.foreachPartition { rows =>
              val conn = getConnection(connectionProperties)
              val cm = new CopyManager(conn.asInstanceOf[BaseConnection])

              cm.copyIn(
                s"""COPY $tableName FROM STDIN WITH (NULL 'null', ENCODING '$encoding', FORMAT CSV, DELIMITER E'$delimiter')""",
                rowsToInputStream(rows)
              )
            }
          }
        else log.warn(s"Table not created in Postgres: $tableName")
      case Failure(e) =>
        closeConnection()
        log.error(s"Error creating/dropping table $tableName")
    }
  }

  def rowsToInputStream(rows: Iterator[Row]): InputStream = {
    val bytes: Iterator[Byte] = rows.flatMap { row =>
      (row.mkString(delimiter).replace("\n", newLineSubstitution) + "\n").getBytes(encoding)
    }

    new InputStream {
      override def read(): Int =
        if (bytes.hasNext) bytes.next & 0xff
        else -1
    }
  }

  override def cleanUp(options: Map[String, String]): Unit = {
    log.info(s"Closing connections in Postgres Output: $name")
    closeConnection()
  }
}