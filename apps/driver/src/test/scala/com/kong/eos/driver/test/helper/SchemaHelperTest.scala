
package com.kong.eos.driver.test.helper

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.cube
import com.kong.eos.sdk.pipeline.aggregation.cube.{Dimension, DimensionType, Precision}
import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.cube.{CubeModel, DimensionModel, OperatorModel}
import com.kong.eos.serving.core.models.policy.writer.WriterModel
import com.kong.eos.serving.core.models.policy.{CheckpointModel, OutputFieldsModel, PolicyElementModel, TransformationModel}
import com.kong.eos.driver.schema.SchemaHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class SchemaHelperTest extends FlatSpec with ShouldMatchers
  with MockitoSugar {

  trait CommonValues {

    val initSchema = StructType(Array(
      StructField("field1", LongType, false),
      StructField("field2", IntegerType, false),
      StructField("field3", StringType, false),
      StructField("field4", StringType, false)))

    val dim1: Dimension = cube.Dimension("dim1", "field1", "", new DimensionTypeTest)
    val dim2: Dimension = cube.Dimension("dim2", "field2", "", new DimensionTypeTest)
    val dimensionTime: Dimension = cube.Dimension("minute", "field3", "minute", new TimeDimensionTypeTest)
    val dimId: Dimension = cube.Dimension("id", "field2", "", new DimensionTypeTest)
    val op1: Operator = new OperatorTest("op1", initSchema, Map())
    val dimension1Model = DimensionModel(
      "dim1", "field1", DimensionType.IdentityName, DimensionType.DefaultDimensionClass, configuration = Some(Map())
    )
    val dimension2Model =
      DimensionModel("dim2", "field2", DimensionType.IdentityName, DimensionType.DefaultDimensionClass)
    val dimensionTimeModel =
      DimensionModel("minute", "field3", DimensionType.TimestampName, DimensionType.TimestampName, Option("10m"))
    val dimensionId = DimensionModel("id", "field2", DimensionType.IdentityName, DimensionType.DefaultDimensionClass)
    val operator1Model = OperatorModel("Count", "op1", Map())
    val output1Model = PolicyElementModel("outputName", "MongoDb", Map())
    val checkpointModel = CheckpointModel("minute", checkpointGranularity, None, 10000)
    val noCheckpointModel = CheckpointModel("none", checkpointGranularity, None, 10000)
    val writerModelId = WriterModel(Seq("outputName"), None, Seq())
    val writerModelTimeDate = WriterModel(Seq("outputName"), Option("date"), Seq())
    val checkpointAvailable = 60000
    val checkpointGranularity = "minute"
    val cubeName = "cubeTest"

    val outputFieldModel1 = OutputFieldsModel("field1", Some("long"))
    val outputFieldModel2 = OutputFieldsModel("field2", Some("int"))
    val outputFieldModel3 = OutputFieldsModel("field3", Some("fake"))
    val outputFieldModel4 = OutputFieldsModel("field4", Some("string"))
    val transformationModel1 =
      TransformationModel("Parser", 0, Some(Input.RawDataKey), Seq(outputFieldModel1, outputFieldModel2))

    val transformationModel2 = TransformationModel("Parser", 1, Some("field1"), Seq(outputFieldModel3,
      outputFieldModel4))
    val writerModel = WriterModel(Seq("outputName"))
  }

  "SchemaHelperTest" should "return a list of schemas" in new CommonValues {
    val cubeModel =
      CubeModel(cubeName, Seq(dimension1Model, dimension2Model, dimensionTimeModel), Seq(operator1Model), writerModel)
    val operators = Seq(op1)
    val dimensions = Seq(dim1, dim2, dimensionTime)
    val cubeSchema = StructType(Array(
      StructField("dim1", StringType, false, SchemaHelper.PkMetadata),
      StructField("dim2", StringType, false, SchemaHelper.PkMetadata),
      StructField(checkpointGranularity, TimestampType, false, SchemaHelper.PkTimeMetadata),
      StructField("op1", LongType, false, SchemaHelper.MeasureMetadata)))

    val res = SchemaHelper.getCubeSchema(cubeModel, operators, dimensions)

    res should be(cubeSchema)
  }

  it should "return a list of schemas without time" in new CommonValues {
    val cubeModel =
      CubeModel(cubeName, Seq(dimension1Model, dimension2Model), Seq(operator1Model), writerModel)
    val operators = Seq(op1)
    val dimensions = Seq(dim1, dim2)
    val cubeSchema = StructType(Array(
      StructField("dim1", StringType, false, SchemaHelper.PkMetadata),
      StructField("dim2", StringType, false, SchemaHelper.PkMetadata),
      StructField("op1", LongType, false, SchemaHelper.MeasureMetadata)))

    val res = SchemaHelper.getCubeSchema(cubeModel, operators, dimensions)

    res should be(cubeSchema)
  }

  it should "return a list of schemas with timeDimension with DateFormat" in
    new CommonValues {
      val cubeModel = CubeModel(cubeName, Seq(dimension1Model, dimension2Model, dimensionTimeModel),
        Seq(operator1Model), writerModelTimeDate)
      val operators = Seq(op1)
      val dimensions = Seq(dim1, dim2, dimensionTime)
      val cubeSchema = StructType(Array(
        StructField("dim1", StringType, false, SchemaHelper.PkMetadata),
        StructField("dim2", StringType, false, SchemaHelper.PkMetadata),
        StructField(checkpointGranularity, DateType, false, SchemaHelper.PkTimeMetadata),
        StructField("op1", LongType, false, SchemaHelper.MeasureMetadata)))

      val res = SchemaHelper.getCubeSchema(cubeModel, operators, dimensions)

      res should be(cubeSchema)
    }

  it should "return a map with the name of the transformation and the schema" in
    new CommonValues {
      val transformationsModel = Seq(transformationModel1, transformationModel2)

      val res = SchemaHelper.getSchemasFromTransformations(transformationsModel, Map())

      val expected = Map(
        "0" -> StructType(Seq(StructField("field1", LongType), StructField("field2", IntegerType))),
        "1" -> StructType(Seq(StructField("field1", LongType), StructField("field2", IntegerType),
          StructField("field3", StringType), StructField("field4", StringType)))
      )

      res should be(expected)
    }

  it should "return a map with the name of the transformation and the schema with the raw" in
    new CommonValues {
      val transformationsModel = Seq(transformationModel1, transformationModel2)

      val res = SchemaHelper.getSchemasFromTransformations(transformationsModel, Input.InitSchema)

      val expected = Map(
        Input.RawDataKey -> StructType(Seq(StructField(Input.RawDataKey, StringType))),
        "0" -> StructType(Seq(StructField(Input.RawDataKey, StringType),
          StructField("field1", LongType),
          StructField("field2", IntegerType))
        ),
        "1" -> StructType(Seq(StructField(Input.RawDataKey, StringType),
          StructField("field1", LongType), StructField("field2", IntegerType),
          StructField("field3", StringType), StructField("field4", StringType)))
      )

      res should be(expected)
    }

  it should "return a schema without the raw" in
    new CommonValues {

      val transformationNoRaw1 =
        TransformationModel("Parser", 0, Some(Input.RawDataKey), Seq(outputFieldModel1, outputFieldModel2),
          Map("removeInputField" -> JsoneyString.apply("true")))
      val transformationNoRaw2 = TransformationModel("Parser", 1, Some("field1"), Seq(outputFieldModel3,
        outputFieldModel4), Map("removeInputField" -> JsoneyString.apply("true")))

      val transformationsModel = Seq(transformationNoRaw1, transformationNoRaw2)

      val schemaWithoutRaw = SchemaHelper.getSchemasFromTransformations(transformationsModel, Input.InitSchema)

      val expected = Map(
        Input.RawDataKey -> StructType(Seq(StructField(Input.RawDataKey, StringType))),
        "0" -> StructType(Seq(StructField("field1", LongType), StructField("field2", IntegerType))),
        "1" -> StructType(Seq(StructField("field2", IntegerType), StructField("field3", StringType),
          StructField("field4", StringType)))
      )

      schemaWithoutRaw should be(expected)
    }

  it should "return the spark date field " in new CommonValues {
    val expected = Output.defaultDateField("field", false)
    val result = SchemaHelper.getTimeFieldType(TypeOp.Date, "field", false)
    result should be(expected)
  }

  it should "return the spark timestamp field " in new CommonValues {
    val expected = Output.defaultTimeStampField("field", false)
    val result = SchemaHelper.getTimeFieldType(TypeOp.Timestamp, "field", false)
    result should be(expected)
  }

  it should "return the spark string field " in new CommonValues {
    val expected = Output.defaultStringField("field", false)
    val result = SchemaHelper.getTimeFieldType(TypeOp.String, "field", false)
    result should be(expected)
  }

  it should "return the spark other field " in new CommonValues {
    val expected = Output.defaultStringField("field", false)
    val result = SchemaHelper.getTimeFieldType(TypeOp.ArrayDouble, "field", false)
    result should be(expected)
  }

  class OperatorTest(name: String,
                     val schema: StructType,
                     properties: Map[String, JSerializable]) extends Operator(name, schema, properties) {

    override val defaultTypeOperation = TypeOp.Long

    override val defaultCastingFilterType = TypeOp.Number

    override def processMap(inputFields: Row): Option[Any] = {
      None
    }

    override def processReduce(values: Iterable[Option[Any]]): Option[Long] = {
      None
    }
  }

  class DimensionTypeTest extends DimensionType {

    override val operationProps: Map[String, JSerializable] = Map()

    override val properties: Map[String, JSerializable] = Map()

    override val defaultTypeOperation = TypeOp.String

    override def precisionValue(keyName: String, value: Any): (Precision, Any) = {
      val precision = DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
      (precision, TypeOp.transformValueByTypeOp(precision.typeOp, value))
    }

    override def precision(keyName: String): Precision =
      DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
  }

  class TimeDimensionTypeTest extends DimensionType {

    override val operationProps: Map[String, JSerializable] = Map()

    override val properties: Map[String, JSerializable] = Map()

    override val defaultTypeOperation = TypeOp.Timestamp

    override def precisionValue(keyName: String, value: Any): (Precision, Any) = {
      val precision = DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
      (precision, TypeOp.transformValueByTypeOp(precision.typeOp, value))
    }

    override def precision(keyName: String): Precision =
      DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
  }

}