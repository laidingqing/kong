
package com.kong.eos.plugin.output.mongodb

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.stratio.datasource.mongodb.config.MongodbConfig
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class MongoDbOutput(name: String, properties: Map[String, JSerializable]) extends Output(name, properties){

  val DefaultHost = "localhost"
  val DefaultPort = "27017"
  val MongoDbSparkDatasource = "com.stratio.datasource.mongodb"
  val hosts = getConnectionConfs("hosts", "host", "port")
  val dbName = properties.getString("dbName", "sparta")

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)
    val primaryKeyOption = getPrimaryKeyOptions(options)
    val dataFrameOptions = getDataFrameOptions(tableName, dataFrame.schema, saveMode, primaryKeyOption)

    validateSaveMode(saveMode)

    dataFrame.write
      .format(MongoDbSparkDatasource)
      .mode(getSparkSaveMode(saveMode))
      .options(dataFrameOptions ++ getCustomProperties)
      .save()
  }

  private def getDataFrameOptions(tableName: String,
                                  schema: StructType,
                                  saveMode: SaveModeEnum.Value,
                                  primaryKey: Option[String]
                                 ): Map[String, String] =
    Map(
      MongodbConfig.Host -> hosts,
      MongodbConfig.Database -> dbName,
      MongodbConfig.Collection -> tableName
    ) ++ {
      saveMode match {
        case SaveModeEnum.Upsert => getUpdateFieldsOptions(schema, primaryKey)
        case _ => Map.empty[String, String]
      }
    }

  private def getUpdateFieldsOptions(schema: StructType, primaryKey: Option[String]): Map[String, String] = {

    val updateFields = primaryKey.getOrElse(
      schema.fields.filter(stField =>
        stField.metadata.contains(Output.PrimaryKeyMetadataKey)).map(_.name).mkString(",")
    )

    Map(MongodbConfig.UpdateFields -> updateFields)
  }

  private def getConnectionConfs(key: String, firstJsonItem: String, secondJsonItem: String): String = {
    val conObj = properties.getMapFromJsoneyString(key)
    conObj.map(c => {
      val host = c.getOrElse(firstJsonItem, DefaultHost)
      val port = c.getOrElse(secondJsonItem, DefaultPort)
      s"$host:$port"
    }).mkString(",")
  }
}
