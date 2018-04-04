
package com.kong.eos.driver.schema

import com.kong.eos.sdk.pipeline.aggregation.cube.{Dimension, ExpiringData}
import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.serving.core.models.policy.TransformationModel
import com.kong.eos.serving.core.models.policy.cube.CubeModel
import org.apache.spark.sql.types.{StructType, _}

import scala.util.Try

object SchemaHelper {

  private val Default_Precision = 10
  private val Default_Scale = 0
  private val Nullable = true
  private val NotNullable = false
  private val MetadataBuilder = new MetadataBuilder
  private val mapTypes = Map(
    TypeOp.Long -> LongType,
    TypeOp.Double -> DoubleType,
    TypeOp.BigDecimal -> DecimalType(Default_Precision, Default_Scale),
    TypeOp.Int -> IntegerType,
    TypeOp.Boolean -> BooleanType,
    TypeOp.Date -> DateType,
    TypeOp.DateTime -> TimestampType,
    TypeOp.Timestamp -> TimestampType,
    TypeOp.ArrayDouble -> ArrayType(DoubleType),
    TypeOp.ArrayString -> ArrayType(StringType),
    TypeOp.String -> StringType,
    TypeOp.MapStringLong -> MapType(StringType, LongType),
    TypeOp.MapStringDouble -> MapType(StringType, DoubleType, valueContainsNull = false))
  private val mapStringSparkTypes = Map(
    "long" -> LongType,
    "double" -> DoubleType,
    "int" -> IntegerType,
    "integer" -> IntegerType,
    "bool" -> BooleanType,
    "boolean" -> BooleanType,
    "date" -> DateType,
    "datetime" -> TimestampType,
    "timestamp" -> TimestampType,
    "string" -> StringType,
    "arraydouble" -> ArrayType(DoubleType),
    "arraystring" -> ArrayType(StringType),
    "text" -> StringType)

  private[driver] val DefaultTimeStampTypeString = "timestamp"
  private[driver] val MeasureMetadata = MetadataBuilder.putBoolean(Output.MeasureMetadataKey, value = true).build()
  private[driver] val PkMetadata = MetadataBuilder.putBoolean(Output.PrimaryKeyMetadataKey, value = true).build()
  private[driver] val PkTimeMetadata = MetadataBuilder.putBoolean(Output.PrimaryKeyMetadataKey, value = true)
    .putBoolean(Output.TimeDimensionKey, value = true).build()
  private[driver] val mapSparkTypes: Map[DataType, TypeOp] = Map(
    LongType -> TypeOp.Long,
    DoubleType -> TypeOp.Double,
    DecimalType(Default_Precision, Default_Scale) -> TypeOp.BigDecimal,
    IntegerType -> TypeOp.Int,
    BooleanType -> TypeOp.Boolean,
    DateType -> TypeOp.Date,
    TimestampType -> TypeOp.Timestamp,
    ArrayType(DoubleType) -> TypeOp.ArrayDouble,
    ArrayType(StringType) -> TypeOp.ArrayString,
    StringType -> TypeOp.String,
    MapType(StringType, LongType) -> TypeOp.MapStringLong,
    MapType(StringType, DoubleType, valueContainsNull = false) -> TypeOp.MapStringDouble)

  def getSchemasFromTransformations(transformationsModel: Seq[TransformationModel],
                                    initSchema: Map[String, StructType]): Map[String, StructType] =
    initSchema ++ searchSchemasFromParsers(transformationsModel.sortBy(_.order), initSchema)

  def getCubeSchema(cubeModel: CubeModel,
                    operators: Seq[Operator],
                    dimensions: Seq[Dimension]): StructType = {
    val measuresMerged = measuresFields(operators, cubeModel.avoidNullValues).sortWith(_.name < _.name)
    val timeDimension = getExpiringData(cubeModel).map(config => config.timeDimension)
    val dimensionsFilterTime = filterDimensionsByTime(dimensions.sorted, timeDimension)
    val dimensionsF = dimensionsFields(dimensionsFilterTime, cubeModel.avoidNullValues)
    val dateType = getTimeTypeFromString(cubeModel.writer.dateType.getOrElse(DefaultTimeStampTypeString))
    val structFields = dimensionsF ++
      timeDimensionFieldType(timeDimension, dateType, cubeModel.avoidNullValues) ++ measuresMerged

    StructType(structFields)
  }

  def getExpiringData(cubeModel: CubeModel): Option[ExpiringData] = {
    val timeDimension = cubeModel.dimensions
      .find(dimensionModel => dimensionModel.computeLast.isDefined)

    timeDimension match {
      case Some(dimensionModelValue) =>
        Option(ExpiringData(
          dimensionModelValue.name,
          dimensionModelValue.precision,
          dimensionModelValue.computeLast.get))
      case _ => None
    }
  }

  def getStreamWriterPkFieldsMetadata(primaryKey: Option[String]): Seq[StructField] =
    primaryKey.fold(Seq.empty[StructField]) { case fields =>
      fields.split(",").map(field => Output.defaultStringField(field, NotNullable, PkMetadata))
    }

  def getTimeTypeFromString(timeType: String): TypeOp =
    timeType.toLowerCase match {
      case "timestamp" => TypeOp.Timestamp
      case "date" => TypeOp.Date
      case "datetime" => TypeOp.DateTime
      case "long" => TypeOp.Long
      case _ => TypeOp.String
    }


  def getTimeFieldType(dateTimeType: TypeOp,
                       fieldName: String,
                       nullable: Boolean,
                       metadata: Option[Metadata] = None): StructField =
    dateTimeType match {
      case TypeOp.Date | TypeOp.DateTime =>
        Output.defaultDateField(fieldName, nullable, metadata.getOrElse(Metadata.empty))
      case TypeOp.Timestamp =>
        Output.defaultTimeStampField(fieldName, nullable, metadata.getOrElse(Metadata.empty))
      case TypeOp.Long =>
        Output.defaultLongField(fieldName, nullable, metadata.getOrElse(Metadata.empty))
      case TypeOp.String =>
        Output.defaultStringField(fieldName, nullable, metadata.getOrElse(Metadata.empty))
      case _ =>
        Output.defaultStringField(fieldName, nullable, metadata.getOrElse(Metadata.empty))
    }

  private[driver] def measuresFields(operators: Seq[Operator], avoidNullValues: Boolean): Seq[StructField] =
    operators.map(operator =>
      StructField(operator.key, rowTypeFromOption(operator.returnType), !avoidNullValues, MeasureMetadata))

  private[driver] def dimensionsFields(fields: Seq[Dimension], avoidNullValues: Boolean): Seq[StructField] =
    fields.map(field =>
      StructField(field.name, rowTypeFromOption(field.precision.typeOp), !avoidNullValues, PkMetadata)
    )

  private[driver] def rowTypeFromOption(optionType: TypeOp): DataType = mapTypes.getOrElse(optionType, StringType)


  private[driver] def searchSchemasFromParsers(transformationsModel: Seq[TransformationModel],
                                               schemas: Map[String, StructType]): Map[String, StructType] =
    transformationsModel.headOption match {
      case Some(transformationModel) =>
        val schema = transformationModel.outputFieldsTransformed.map(outputField =>
          outputField.name -> StructField(outputField.name,
            mapStringSparkTypes.getOrElse(outputField.`type`.toLowerCase, StringType),
            Nullable
          )
        )

        val inputFields = schemas.values.flatMap(structType => structType.fields)
        val fieldsFiltered = {
          if (Try(transformationModel.configuration.getBoolean("removeInputField")).getOrElse(false) &&
            transformationModel.inputField.isDefined)
            inputFields.filter(stField => stField.name != transformationModel.inputField.get)
          else inputFields
        }

        val fields = fieldsFiltered ++ schema.map(_._2)
        val recursiveSchema = Map(transformationModel.order.toString -> StructType(fields.toSeq))

        if (transformationsModel.size == 1)
          schemas ++ recursiveSchema
        else schemas ++ searchSchemasFromParsers(transformationsModel.drop(1), recursiveSchema)
      case None =>
        schemas
    }

  private[driver] def filterDimensionsByTime(dimensions: Seq[Dimension],
                                             timeDimension: Option[String]): Seq[Dimension] =
    timeDimension match {
      case Some(timeName) => dimensions.filter(dimension => dimension.name != timeName)
      case None => dimensions
    }

  private[driver] def timeDimensionFieldType(timeDimension: Option[String],
                                             dateType: TypeOp.Value,
                                             avoidNullValues: Boolean): Seq[StructField] = {
    timeDimension match {
      case None =>
        Seq.empty[StructField]
      case Some(timeDimensionName) =>
        Seq(getTimeFieldType(dateType, timeDimensionName, !avoidNullValues, Some(PkTimeMetadata)))
    }
  }
}