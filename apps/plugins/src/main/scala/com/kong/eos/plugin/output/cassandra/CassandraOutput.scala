
package com.kong.eos.plugin.output.cassandra

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._

class CassandraOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties) {

  val MaxTableNameLength = 48

  val keyspace = properties.getString("keyspace", "sparta")
  val cluster = properties.getString("cluster", "default")

  override def supportedSaveModes: Seq[SaveModeEnum.Value] =
    Seq(SaveModeEnum.Append, SaveModeEnum.ErrorIfExists, SaveModeEnum.Ignore, SaveModeEnum.Overwrite)

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableNameVersioned = getTableName(getTableNameFromOptions(options).toLowerCase)

    validateSaveMode(saveMode)

    dataFrame.write
      .format("org.apache.spark.sql.cassandra")
      .mode(getSparkSaveMode(saveMode))
      .options(Map("table" -> tableNameVersioned, "keyspace" -> keyspace, "cluster" -> cluster) ++ getCustomProperties
      )
      .save()
  }

  def getTableName(table: String): String =
    if (table.length > MaxTableNameLength - 3) table.substring(0, MaxTableNameLength - 3) else table

}

object CassandraOutput {

  final val DefaultHost = "127.0.0.1"
  final val DefaultPort = "9042"

  def getSparkConfiguration(configuration: Map[String, JSerializable]): Seq[(String, String)] = {
    val connectionHost = configuration.getString("connectionHost", DefaultHost)
    val connectionPort = configuration.getString("connectionPort", DefaultPort)

    val sparkProperties = getSparkCassandraProperties(configuration)

    sparkProperties ++
      Seq(
        ("spark.cassandra.connection.host", connectionHost),
        ("spark.cassandra.connection.port", connectionPort)
      )
  }

  private def getSparkCassandraProperties(configuration: Map[String, JSerializable]): Seq[(String, String)] = {
    configuration.get("sparkProperties") match {
      case Some(properties) =>
        val conObj = configuration.getMapFromJsoneyString("sparkProperties")
        conObj.map(propKeyPair => {
          val key = propKeyPair("sparkPropertyKey")
          val value = propKeyPair("sparkPropertyValue")
          (key, value)
        })
      case None => Seq()
    }
  }
}
