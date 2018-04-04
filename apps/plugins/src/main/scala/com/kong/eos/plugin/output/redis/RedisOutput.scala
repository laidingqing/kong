
package com.kong.eos.plugin.output.redis

import java.io.Serializable
import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.output.Output._
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, Row}
import com.kong.eos.plugin.output.redis.dao.AbstractRedisDAO
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Saves calculated cubes on Redis.
 * The hashKey will have this kind of structure -> A:valueA:B:valueB:C:valueC.It is important to see that
 * values will be part of the key and the objective of it is to perform better searches in the hash.
 *
 */
class RedisOutput(name: String, properties: Map[String, Serializable])
  extends Output(name, properties) with AbstractRedisDAO with Serializable {

  override val hostname = properties.getString("hostname", DefaultRedisHostname)
  override val port = properties.getString("port", DefaultRedisPort).toInt

  override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)
    val schema = dataFrame.schema

    validateSaveMode(saveMode)

    dataFrame.foreachPartition{ rowList =>
      rowList.foreach{ row =>
        val valuesList = getValuesList(row,schema.fieldNames)
        val hashKey = getHashKeyFromRow(valuesList, schema)
        getMeasuresFromRow(valuesList, schema).foreach { case (measure, value) =>
          hset(hashKey, measure.name, value)
        }
      }
    }
  }

  def getHashKeyFromRow(valuesList: Seq[(String, String)], schema: StructType): String =
    valuesList.flatMap{ case (key, value) =>
      val fieldSearch = schema.fields.find(structField =>
        structField.metadata.contains(Output.PrimaryKeyMetadataKey) && structField.name == key)

      fieldSearch.map(structField => s"${structField.name}$IdSeparator$value")
    }.mkString(IdSeparator)

  def getMeasuresFromRow(valuesList: Seq[(String, String)], schema: StructType): Seq[(StructField, String)] =
    valuesList.flatMap{ case (key, value) =>
      val fieldSearch = schema.fields.find(structField =>
          structField.metadata.contains(Output.MeasureMetadataKey) &&
          structField.name == key)
      fieldSearch.map(field => (field, value))
    }

  def getValuesList(row: Row, fieldNames: Array[String]): Seq[(String, String)] =
    fieldNames.zip(row.toSeq).map{ case (key, value) => (key, value.toString)}.toSeq
}