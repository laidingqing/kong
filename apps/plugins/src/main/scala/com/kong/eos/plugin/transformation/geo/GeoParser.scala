
package com.kong.eos.plugin.transformation.geo

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.util.Try

class GeoParser(
                 order: Integer,
                 inputField: Option[String],
                 outputFields: Seq[String],
                 schema: StructType,
                 properties: Map[String, JSerializable]
               ) extends Parser(order, inputField, outputFields, schema, properties) {

  val defaultLatitudeField = "latitude"
  val defaultLongitudeField = "longitude"
  val separator = "__"

  val latitudeField = properties.getOrElse("latitude", defaultLatitudeField).toString
  val longitudeField = properties.getOrElse("longitude", defaultLongitudeField).toString

  def parse(row: Row): Seq[Row] = {
    val newData = Try {
      val geoValue = geoField(getLatitude(row), getLongitude(row))
      outputFields.map(outputField => {
        val outputSchemaValid = outputFieldsSchema.find(field => field.name == outputField)
        outputSchemaValid match {
          case Some(outSchema) =>
            TypeOp.transformValueByTypeOp(outSchema.dataType, geoValue)
          case None =>
            returnWhenError(
              throw new IllegalStateException(s"Impossible to parse outputField: $outputField in the schema"))
        }
      })
    }

    returnData(newData, removeInputField(row))
  }

  private def getLatitude(row: Row): String = {
    val latitude = Try(row.get(schema.fieldIndex(latitudeField)))
      .getOrElse(throw new RuntimeException(s"Impossible to parse $latitudeField in the event: ${row.mkString(",")}"))

    latitude match {
      case valueCast: String => valueCast
      case valueCast: Array[Byte] => new Predef.String(valueCast)
      case _ => latitude.toString
    }
  }

  private def getLongitude(row: Row): String = {
    val longitude = Try(row.get(schema.fieldIndex(longitudeField)))
      .getOrElse(throw new RuntimeException(s"Impossible to parse $latitudeField in the event: ${row.mkString(",")}"))

    longitude match {
      case valueCast: String => valueCast
      case valueCast: Array[Byte] => new Predef.String(valueCast)
      case _ => longitude.toString
    }
  }

  private def geoField(latitude: String, longitude: String): String = latitude + separator + longitude
}
