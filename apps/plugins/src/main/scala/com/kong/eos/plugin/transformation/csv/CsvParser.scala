
package com.kong.eos.plugin.transformation.csv

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.util.Try

class CsvParser(order: Integer,
                 inputField: Option[String],
                 outputFields: Seq[String],
                 schema: StructType,
                 properties: Map[String, JSerializable])
  extends Parser(order, inputField, outputFields, schema, properties) {

  val fieldsModel = properties.getPropertiesFields("fields")
  val fieldsSeparator = Try(properties.getString("delimiter")).getOrElse(",")

  //scalastyle:off
  override def parse(row: Row): Seq[Row] = {
    val inputValue = Option(row.get(inputFieldIndex))
    val newData = Try {
      inputValue match {
        case Some(value) =>
          val valuesSplitted = {
            value match {
              case valueCast: Array[Byte] => new Predef.String(valueCast)
              case valueCast: String => valueCast
              case _ => value.toString
            }
          }.split(fieldsSeparator)

          if(valuesSplitted.length == fieldsModel.fields.length){
            val valuesParsed = fieldsModel.fields.map(_.name).zip(valuesSplitted).toMap
            outputFields.map { outputField =>
              val outputSchemaValid = outputFieldsSchema.find(field => field.name == outputField)
              outputSchemaValid match {
                case Some(outSchema) =>
                  valuesParsed.get(outSchema.name) match {
                    case Some(valueParsed) =>
                      parseToOutputType(outSchema, valueParsed)
                    case None =>
                      returnWhenError(new IllegalStateException(
                        s"The values parsed don't contain the schema field: ${outSchema.name}"))
                  }
                case None =>
                  returnWhenError(new IllegalStateException(
                    s"Impossible to parse outputField: $outputField in the schema"))
              }
            }
          } else returnWhenError(new IllegalStateException(s"The values splitted are greater or lower than the properties fields"))
        case None =>
          returnWhenError(new IllegalStateException(s"The input value is null or empty"))
      }
    }

    returnData(newData, removeInputField(row))
  }

  //scalastyle:on
}
