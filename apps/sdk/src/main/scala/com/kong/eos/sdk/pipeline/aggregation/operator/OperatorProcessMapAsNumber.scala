package com.kong.eos.sdk.pipeline.aggregation.operator

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.filter.Filter
import com.kong.eos.sdk.pipeline.schema.TypeOp
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

trait OperatorProcessMapAsNumber extends SLF4JLogging {

  self: Filter =>

  val inputSchema: StructType

  val inputField: Option[String]

  def processMap(inputFieldsValues: Row): Option[Number] =
    if (inputField.isDefined && inputSchema.fieldNames.contains(inputField.get))
      applyFilters(inputFieldsValues)
        .map(filteredFields => getNumberFromAny(filteredFields.get(inputField.get).get))
    else None

  /**
   * This method tries to cast a value to Number, if it's possible.
   *
   * Serializable -> String -> Number
   * Serializable -> Number
   *
   */
  def getNumberFromAny(value: Any): Number =
    Try(TypeOp.transformValueByTypeOp(TypeOp.Double, value).asInstanceOf[Number]) match {
      case Success(number) => number
      case Failure(ex) => log.info(s"Impossible to parse as double number inside operator: ${value.toString}")
        throw new Exception(ex.getMessage)
    }
}
