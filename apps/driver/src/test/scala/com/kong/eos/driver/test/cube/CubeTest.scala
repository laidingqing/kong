
package com.kong.eos.driver.test.cube

import com.kong.eos.plugin.cube.operator.count.CountOperator
import com.kong.eos.plugin.cube.operator.sum.SumOperator
import com.kong.eos.sdk.pipeline.aggregation.cube._
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.utils.AggregationTime
import com.kong.eos.driver.step
import com.kong.eos.driver.step.{Cube, Trigger}
import com.kong.eos.driver.writer.WriterOptions
import com.kong.eos.plugin.cube.field.defaultField.DefaultField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.TestSuiteBase
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class CubeTest extends TestSuiteBase {

  test("aggregate with time") {

    val PreserverOrder = true
    val defaultDimension = new DefaultField
    val checkpointTimeAvailability = "600000ms"
    val checkpointGranularity = "minute"
    val eventGranularity = AggregationTime.truncateDate(DateTime.now(), "minute")
    val name = "cubeName"
    val timeConfig = Option(TimeConfig(eventGranularity, checkpointGranularity))

    val expiringDataConfig = ExpiringData(checkpointGranularity, checkpointGranularity, checkpointTimeAvailability)
    val initSchema = StructType(Seq(
      StructField("n", LongType, false)
    ))
    val operatorCount = new CountOperator("count", StructType(Seq(StructField("count", LongType, true))), Map())
    val operatorSum =
      new SumOperator("sum", StructType(Seq(StructField("n", LongType, true))), Map("inputField" -> "n"))


    val cube = Cube(
      name,
      Seq(Dimension("dim1", "foo", "identity", defaultDimension)),
      Seq(operatorCount, operatorSum),
      initSchema,
      initSchema,
      TypeOp.Timestamp,
      Option(expiringDataConfig),
      Seq.empty[Trigger],
      WriterOptions()
    )

    testOperation(getInput, cube.aggregate, getOutput, PreserverOrder)

    def getInput: Seq[Seq[(DimensionValuesTime, InputFields)]] = Seq(Seq(
      (DimensionValuesTime("testCube",
        Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar")),
        timeConfig), InputFields(Row(4), 1)),

      (DimensionValuesTime("testCube",
        Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar")), timeConfig),
        InputFields(Row(3), 1)),

      (DimensionValuesTime("testCube",
        Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo")), timeConfig),
        InputFields(Row(3), 1))),

      Seq(
        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar")), timeConfig),
          InputFields(Row(4), 1)),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar")), timeConfig),
          InputFields(Row(3), 1)),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo")), timeConfig),
          InputFields(Row(3), 1))))

    def getOutput: Seq[Seq[(DimensionValuesTime, MeasuresValues)]] = Seq(
      Seq(
        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar")),
          timeConfig),
          MeasuresValues(Map("count" -> Some(2L), "sum" -> Some(7L)))),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo")),
          timeConfig),
          MeasuresValues(Map("count" -> Some(1L), "sum" -> Some(3L))))),

      Seq(
        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar")),
          timeConfig),
          MeasuresValues(Map("count" -> Some(4L), "sum" -> Some(14L)))),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo")),
          timeConfig),
          MeasuresValues(Map("count" -> Some(2L), "sum" -> Some(6L))))))
  }

  test("aggregate without time") {

    val PreserverOrder = true
    val defaultDimension = new DefaultField
    val name = "cubeName"

    val initSchema = StructType(Seq(
      StructField("n", StringType, false)
    ))
    val operatorCount = new CountOperator("count", StructType(Seq(StructField("count", LongType, true))), Map())
    val operatorSum =
      new SumOperator("sum", StructType(Seq(StructField("n", LongType, true))), Map("inputField" -> "n"))

    val cube = step.Cube(
      name,
      Seq(Dimension("dim1", "foo", "identity", defaultDimension)),
      Seq(operatorCount, operatorSum),
      initSchema,
      initSchema,
      TypeOp.Timestamp,
      expiringDataConfig = None,
      Seq.empty[Trigger],
      WriterOptions()
    )

    testOperation(getInput, cube.aggregate, getOutput, PreserverOrder)

    def getInput: Seq[Seq[(DimensionValuesTime, InputFields)]] = Seq(Seq(
      (DimensionValuesTime("testCube",
        Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar"))), InputFields(Row(4), 1)),

      (DimensionValuesTime("testCube",
        Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar"))), InputFields(Row(3), 1)),

      (DimensionValuesTime("testCube",
        Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo"))), InputFields(Row(3), 1))),

      Seq(
        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar"))), InputFields(Row(4), 1)),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar"))), InputFields(Row(3), 1)),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo"))), InputFields(Row(3), 1))))

    def getOutput: Seq[Seq[(DimensionValuesTime, MeasuresValues)]] = Seq(
      Seq(
        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar"))),
          MeasuresValues(Map("count" -> Some(2L), "sum" -> Some(7L)))),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo"))),
          MeasuresValues(Map("count" -> Some(1L), "sum" -> Some(3L))))),

      Seq(
        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "bar"))),
          MeasuresValues(Map("count" -> Some(4L), "sum" -> Some(14L)))),

        (DimensionValuesTime("testCube",
          Seq(DimensionValue(Dimension("dim1", "foo", "identity", defaultDimension), "foo"))),
          MeasuresValues(Map("count" -> Some(2L), "sum" -> Some(6L))))))
  }
}
