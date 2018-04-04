
package com.kong.eos.plugin.transformation.xpath

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.sdk.properties.models.PropertiesQueriesModel
import kantan.xpath.XPathResult
import kantan.xpath._
import kantan.xpath.ops._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import scala.util.Try

class XPathParser(order: Integer,
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
            case valueCast: Array[Byte] => XPathParser.xPathParse(new Predef.String(valueCast), queriesModel)
            case valueCast: String => XPathParser.xPathParse(valueCast, queriesModel)
            case _ => XPathParser.xPathParse(value.toString, queriesModel)
          }

          outputFields.map { outputField =>
            val outputSchemaValid = outputFieldsSchema.find(field => field.name == outputField)
            outputSchemaValid match {
              case Some(outSchema) =>
                valuesParsed.get(outSchema.name) match {
                  case Some(valueParsed) =>
                    parseToOutputType(outSchema, valueParsed)
                  case None =>
                    returnWhenError(new IllegalStateException(
                      s"The values parsed not have the schema field: ${outSchema.name}"))
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

object XPathParser {

  def xPathParse(xmlData: String, queriesModel: PropertiesQueriesModel): Map[String, Any] =
    queriesModel.queries.map(queryModel => (queryModel.field, parse[String](xmlData, queryModel.query))).toMap

  /**
   * Receives a query and returns the elements found. If there was an error,
   * an exception will be thrown
   *
   * @param query search to apply
   * @tparam T type of the elements returned
   * @return elements found
   */
  private def parse[T: Compiler](source: String, query: String): T =
    applyQuery(source, query).get

  /**
   * Receives a query and returns an XPathResult with the try of apply
   * the query to the source value
   *
   * @param query search to apply
   * @tparam T type of the elements returned.
   * @return a XPathResult with the try of apply the query
   */
  private def applyQuery[T: Compiler](source: String, query: String): XPathResult[T] =
    source.evalXPath[T](query)
}
