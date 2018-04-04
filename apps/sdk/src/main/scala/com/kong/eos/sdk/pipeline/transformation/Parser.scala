
package com.kong.eos.sdk.pipeline.transformation

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.properties.{CustomProperties, Parameterizable}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.{Failure, Success, Try}

abstract class Parser(order: Integer,
                      inputField: Option[String],
                      outputFields: Seq[String],
                      schema: StructType,
                      properties: Map[String, JSerializable])
  extends Parameterizable(properties) with Ordered[Parser] with CustomProperties {

  val customKey = "transformationOptions"
  val customPropertyKey = "transformationOptionsKey"
  val customPropertyValue = "transformationOptionsValue"
  val propertiesWithCustom = properties ++ getCustomProperties

  val outputFieldsSchema = schema.fields.filter(field => outputFields.contains(field.name))

  val inputFieldRemoved = Try(propertiesWithCustom.getBoolean("removeInputField")).getOrElse(false)

  val inputFieldIndex = inputField match {
    case Some(field) => Try(schema.fieldIndex(field)).getOrElse(0)
    case None => 0
  }

  val whenErrorDo = Try(WhenError.withName(propertiesWithCustom.getString("whenError")))
    .getOrElse(WhenError.Error)

  def parse(data: Row): Seq[Row]

  def getOrder: Integer = order

  def checkFields(keyMap: Map[String, JSerializable]): Map[String, JSerializable] =
    keyMap.flatMap(key => if (outputFields.contains(key._1)) Some(key) else None)

  def compare(that: Parser): Int = this.getOrder.compareTo(that.getOrder)

  //scalastyle:off
  def returnWhenError(exception: Exception): Null =
  whenErrorDo match {
    case WhenError.Null => null
    case _ => throw exception
  }

  //scalastyle:on

  def parseToOutputType(outSchema: StructField, inputValue: Any): Any =
    Try(TypeOp.transformValueByTypeOp(outSchema.dataType, inputValue.asInstanceOf[Any]))
      .getOrElse(returnWhenError(new IllegalStateException(
        s"Error parsing to output type the value: ${inputValue.toString}")))

  def returnData(newData: Try[Seq[_]], prevData: Seq[_]): Seq[Row] =
    newData match {
      case Success(data) => Seq(Row.fromSeq(prevData ++ data))
      case Failure(e) => whenErrorDo match {
        case WhenError.Discard => Seq.empty[Row]
        case _ => throw e
      }
    }

  def returnData(newData: Try[Row], prevData: Row): Seq[Row] =
    newData match {
      case Success(data) => Seq(Row.merge(prevData, data))
      case Failure(e) => whenErrorDo match {
        case WhenError.Discard => Seq.empty[Row]
        case _ => throw e
      }
    }

  def removeIndex(row: Seq[_], inputFieldIndex: Int): Seq[_] = if (row.size < inputFieldIndex) row
    else row.take(inputFieldIndex) ++ row.drop(inputFieldIndex + 1)

  def removeInputField(row: Row): Seq[_] = {
    if (inputFieldRemoved && inputField.isDefined)
      removeIndex(row.toSeq, inputFieldIndex)
    else
      row.toSeq
  }


}

object Parser {

  final val ClassSuffix = "Parser"
  final val DefaultOutputType = "string"
  final val TypesFromParserClass = Map("datetime" -> "timestamp")
}
