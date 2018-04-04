
package com.kong.eos.driver.test.writer

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.cube
import com.kong.eos.sdk.pipeline.aggregation.cube._
import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.driver
import com.kong.eos.driver.step.{Cube, Trigger}
import com.kong.eos.driver.writer.{CubeWriterHelper, WriterOptions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CubeWriterTest extends FlatSpec with ShouldMatchers {

  "CubeWriterTest" should "return a row with values and timeDimension" in
    new CommonValues {
      val schema = StructType(Array(
        StructField("dim1", StringType, false),
        StructField("dim2", StringType, false),
        StructField(checkpointGranularity, TimestampType, false),
        StructField("op1", LongType, true)))
      val cube = Cube(cubeName, Seq(dim1, dim2), Seq(op1), initSchema, schema, TypeOp.Timestamp,
        Option(ExpiringData("minute", checkpointGranularity, "100000ms")), Seq.empty[Trigger], WriterOptions())

      val writerOptions = WriterOptions(Seq("outputName"))
      val output = new OutputMock("outputName", Map())
      val res = CubeWriterHelper.toRow(cube, dimensionValuesT, measures)

      res should be(Row.fromSeq(Seq("value1", "value2", 1L, "value")))
    }

  "CubeWriterTest" should "return a row with values without timeDimension" in
    new CommonValues {
      val schema = StructType(Array(
        StructField("dim1", StringType, false),
        StructField("dim2", StringType, false),
        StructField("op1", LongType, true)))
      val cube = Cube(cubeName, Seq(dim1, dim2), Seq(op1), initSchema, schema, TypeOp.Timestamp, None,
        Seq.empty[Trigger], WriterOptions())
      val writerOptions = WriterOptions(Seq("outputName"))
      val output = new OutputMock("outputName", Map())
      val res = CubeWriterHelper.toRow(cube, dimensionValuesNoTime, measures)

      res should be(Row.fromSeq(Seq("value1", "value2", "value")))
    }

  "CubeWriterTest" should "return a row with values with noTime" in
    new CommonValues {
      val schema = StructType(Array(
        StructField("dim1", StringType, false),
        StructField("dim2", StringType, false),
        StructField("op1", LongType, true)))
      val cube = driver.step.Cube(cubeName, Seq(dim1, dim2), Seq(op1), initSchema, schema, TypeOp.Timestamp,
        None, Seq.empty[Trigger], WriterOptions())
      val writerOptions = WriterOptions(Seq("outputName"))
      val output = new OutputMock("outputName", Map())
      val res = CubeWriterHelper.toRow(cube, dimensionValuesNoTime, measures)

      res should be(Row.fromSeq(Seq("value1", "value2", "value")))
    }

  "CubeWriterTest" should "return a row with values with time" in
    new CommonValues {
      val schema = StructType(Array(
        StructField("dim1", StringType, false),
        StructField("dim2", StringType, false),
        StructField("op1", LongType, true)))
      val cube = driver.step.Cube(cubeName, Seq(dim1, dim2), Seq(op1), initSchema, schema, TypeOp.Timestamp,
        None, Seq.empty[Trigger], WriterOptions())
      val writerOptions = WriterOptions(Seq("outputName"))
      val output = new OutputMock("outputName", Map())
      val res = CubeWriterHelper.toRow(cube, dimensionValuesT, measures)

      res should be(Row.fromSeq(Seq("value1", "value2", 1L, "value")))
    }

  class OperatorTest(name: String, val schema: StructType, properties: Map[String, JSerializable])
    extends Operator(name, schema, properties) {

    override val defaultTypeOperation = TypeOp.Long

    override val defaultCastingFilterType = TypeOp.Number

    override def processMap(inputFields: Row): Option[Any] = {
      None
    }

    override def processReduce(values: Iterable[Option[Any]]): Option[Long] = {
      None
    }
  }

  class OutputMock(keyName: String, properties: Map[String, JSerializable])
    extends Output(keyName, properties) {

    override def save(dataFrame: DataFrame, saveMode: SaveModeEnum.Value, options: Map[String, String]): Unit = {}
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

  trait CommonValues {

    val dim1: Dimension = cube.Dimension("dim1", "field1", "", new DimensionTypeTest)
    val dim2: Dimension = cube.Dimension("dim2", "field2", "", new DimensionTypeTest)
    val dimId: Dimension = cube.Dimension("id", "field2", "", new DimensionTypeTest)
    val op1: Operator = new OperatorTest("op1", StructType(Seq(StructField("n", LongType, false))), Map())
    val checkpointAvailable = 60000
    val checkpointGranularity = "minute"
    val cubeName = "cubeTest"
    val defaultDimension = new DimensionTypeTest
    val dimensionValuesT = DimensionValuesTime("testCube", Seq(DimensionValue(
      cube.Dimension("dim1", "eventKey", "identity", defaultDimension), "value1"),
      DimensionValue(
        cube.Dimension("dim2", "eventKey", "identity", defaultDimension), "value2"),
      DimensionValue(
        cube.Dimension("minute", "eventKey", "identity", defaultDimension), 1L)))

    val dimensionValuesNoTime = DimensionValuesTime("testCube", Seq(DimensionValue(
      cube.Dimension("dim1", "eventKey", "identity", defaultDimension), "value1"),
      DimensionValue(
        cube.Dimension("dim2", "eventKey", "identity", defaultDimension), "value2")))

    val measures = MeasuresValues(Map("field" -> Option("value")))
    val initSchema = StructType(Seq(StructField("n", StringType, false)))
  }

}
