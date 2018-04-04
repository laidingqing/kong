
package com.kong.eos.driver.writer

import com.kong.eos.sdk.pipeline.autoCalculations.AutoCalculatedField
import com.kong.eos.sdk.pipeline.output.Output
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameModifierHelper {

  def applyAutoCalculateFields(dataFrame: DataFrame,
                               autoCalculateFields: Seq[AutoCalculatedField],
                               auxSchema: StructType): DataFrame =
    autoCalculateFields.headOption match {
      case Some(firstAutoCalculate) =>
        applyAutoCalculateFields(
          addColumnToDataFrame(dataFrame, firstAutoCalculate, auxSchema), autoCalculateFields.drop(1), auxSchema)
      case None =>
        dataFrame
    }

  private[driver] def addColumnToDataFrame(dataFrame: DataFrame,
                                   autoCalculateField: AutoCalculatedField,
                                   auxSchema: StructType): DataFrame = {
    (autoCalculateField.fromNotNullFields,
      autoCalculateField.fromPkFields,
      autoCalculateField.fromFields,
      autoCalculateField.fromFixedValue) match {
      case (Some(fromNotNullFields), _, _, _) =>
        val fields = fieldsWithAuxMetadata(dataFrame.schema.fields, auxSchema.fields).flatMap(field =>
          if (!field.nullable) Some(col(field.name)) else None).toSeq
        addField(fromNotNullFields.field.name, fromNotNullFields.field.outputType, dataFrame, fields)
      case (None, Some(fromPkFields), _, _) =>
        val fields = fieldsWithAuxMetadata(dataFrame.schema.fields, auxSchema.fields).flatMap(field =>
          if (field.metadata.contains(Output.PrimaryKeyMetadataKey)) Some(col(field.name)) else None).toSeq
        addField(fromPkFields.field.name, fromPkFields.field.outputType, dataFrame, fields)
      case (None, None, Some(fromFields), _) =>
        val fields = autoCalculateField.fromFields.get.fromFields.map(field => col(field))
        addField(fromFields.field.name, fromFields.field.outputType, dataFrame, fields)
      case (None, None, None, Some(fromFixedValue)) =>
        addLiteral(fromFixedValue.field.name, fromFixedValue.field.outputType, dataFrame, fromFixedValue.value)
      case _ => dataFrame
    }
  }

  private[driver] def addField(name: String, outputType: String, dataFrame: DataFrame, fields: Seq[Column]): DataFrame =
    outputType match {
      case "string" => dataFrame.withColumn(name, concat_ws(Output.Separator, fields: _*))
      case "array" => dataFrame.withColumn(name, array(fields: _*))
      case "map" => dataFrame.withColumn(name, struct(fields: _*))
      case _ => dataFrame
    }

  private[driver] def addLiteral(name: String, outputType: String, dataFrame: DataFrame, literal: String): DataFrame =
    outputType match {
      case "string" => dataFrame.withColumn(name, lit(literal))
      case "array" => dataFrame.withColumn(name, array(lit(literal)))
      case "map" => dataFrame.withColumn(name, struct(lit(literal)))
      case _ => dataFrame
    }

  private[driver] def fieldsWithAuxMetadata(dataFrameFields: Array[StructField], auxFields: Array[StructField]) =
    dataFrameFields.map(field => {
      auxFields.find(auxField => auxField.name == field.name) match {
        case Some(auxFounded) => field.copy(metadata = auxFounded.metadata)
        case None => field
      }
    })
}
