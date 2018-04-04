
package com.kong.eos.plugin.transformation.json

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.transformation.WhenError.WhenError
import com.kong.eos.sdk.pipeline.transformation.{Parser, WhenError}
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.sdk.properties.models.PropertiesQueriesModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.util.Try

class JsonParser(order: Integer,
                 inputField: Option[String],
                 outputFields: Seq[String],
                 schema: StructType,
                 properties: Map[String, JSerializable])
  extends Parser(order, inputField, outputFields, schema, properties) {

  val queriesModel = properties.getPropertiesQueries("queries")

  //scalastyle:off
  override def parse(row: Row): Seq[Row] = {
    val inputValue = Option(row.get(inputFieldIndex))
    val newData = Try {
      inputValue match {
        case Some(value) =>
          val valuesParsed = value match {
          case valueCast: Array[Byte] => JsonParser.jsonParse(new Predef.String(valueCast), queriesModel, whenErrorDo)
          case valueCast: String => JsonParser.jsonParse(valueCast, queriesModel, whenErrorDo)
          case _ => JsonParser.jsonParse(value.toString, queriesModel, whenErrorDo)
        }

          outputFields.map { outputField =>
            val outputSchemaValid = outputFieldsSchema.find(field => field.name == outputField)
            outputSchemaValid match {
              case Some(outSchema) =>
                valuesParsed.get(outSchema.name) match {
                  case Some(valueParsed) if valueParsed != null =>
                    parseToOutputType(outSchema, valueParsed)
                  case _ =>
                    returnWhenError(new IllegalStateException(
                      s"The values parsed don't contain the schema field: ${outSchema.name}"))
                }
              case None =>
                returnWhenError(new IllegalStateException(
                  s"Impossible to parse outputField: $outputField in the schema"))
            }
          }
        case None =>
          returnWhenError(new IllegalStateException(s"The input value is null or empty"))
      }
    }

    returnData(newData, removeInputField(row))
  }

  //scalastyle:on
}

object JsonParser {

  def jsonParse(jsonData: String,
                queriesModel: PropertiesQueriesModel,
                whenError: WhenError = WhenError.Null): Map[String, Any] = {
    val jsonPathExtractor = new JsonPathExtractor(jsonData, whenError == WhenError.Null)

    queriesModel.queries.map(queryModel => (queryModel.field, jsonPathExtractor.query(queryModel.query))).toMap
  }
}
