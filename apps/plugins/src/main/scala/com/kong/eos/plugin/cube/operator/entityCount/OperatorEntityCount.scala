
package com.kong.eos.plugin.cube.operator.entityCount

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType



abstract class OperatorEntityCount(name: String,
                                   val schema: StructType,
                                   properties: Map[String, JSerializable])
  extends Operator(name, schema, properties) {

  val split = if (properties.contains("split")) Some(properties.getString("split")) else None

  val replaceRegex =
    if (properties.contains("replaceRegex")) Some(properties.getString("replaceRegex")) else None

  override def processMap(inputFieldsValues: Row): Option[Seq[String]] = {
    if (inputField.isDefined && schema.fieldNames.contains(inputField.get))
      applyFilters(inputFieldsValues).flatMap(filteredFields => filteredFields.get(inputField.get).map(applySplitters))
    else None
  }

  private def applySplitters(value: Any): Seq[String] = {
    val replacedValue = applyReplaceRegex(value.toString)
    if (split.isDefined) replacedValue.split(split.get) else Seq(replacedValue)
  }

  private def applyReplaceRegex(value: String): String =
    replaceRegex match {
      case Some(regex) => value.replaceAll(regex, "")
      case None => value
    }
}
