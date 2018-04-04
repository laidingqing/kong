package com.kong.eos.sdk.pipeline.aggregation.operator

import com.kong.eos.sdk.pipeline.filter.Filter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

trait OperatorProcessMapAsAny {

  self: Filter =>

  val inputSchema: StructType

  val inputField: Option[String]

  def processMap(inputFieldsValues: Row): Option[Any] =
    if (inputField.isDefined && inputSchema.fieldNames.contains(inputField.get))
      applyFilters(inputFieldsValues).flatMap(filteredFields => filteredFields.get(inputField.get))
    else None
}
