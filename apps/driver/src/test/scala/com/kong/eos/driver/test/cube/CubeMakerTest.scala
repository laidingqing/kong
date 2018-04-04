
package com.kong.eos.driver.test.cube

import java.sql.Timestamp

import com.kong.eos.plugin.cube.field.datetime.DateTimeField
import com.kong.eos.plugin.cube.operator.count.CountOperator
import com.kong.eos.sdk.pipeline.aggregation
import com.kong.eos.sdk.pipeline.aggregation.cube
import com.kong.eos.sdk.pipeline.aggregation.cube.{Dimension, DimensionValue, DimensionValuesTime, InputFields}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.utils.AggregationTime
import com.github.nscala_time.time.Imports._
import com.kong.eos.driver.step.{Cube, CubeOperations, Trigger}
import com.kong.eos.driver.writer.WriterOptions
import com.kong.eos.plugin.cube.field.defaultField.DefaultField
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.streaming.TestSuiteBase
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CubeMakerTest extends TestSuiteBase {

  val PreserverOrder = false

  /**
   * Given a cube defined with:
    - D = A dimension with name eventKey and a string value.
    - B = A DefaultDimension applied to the dimension
    - O = No operator for the cube
    - R = Cube with D+B+O

    This test should produce Seq[(Seq[DimensionValue], Map[String, JSerializable])]
   */
  test("DataCube extracts dimensions from events") {

    val checkpointTimeAvailability = 600000
    val checkpointGranularity = "minute"
    val timeDimensionName = "minute"
    val millis = AggregationTime.truncateDate(DateTime.now, checkpointGranularity)
    val sqlTimestamp = new Timestamp(millis)
    val name = "cubeName"
    val operator = new CountOperator("count", StructType(Seq(StructField("count", LongType, true))), Map())
    val multiplexer = false
    val defaultDimension = new DefaultField
    val timeField = new DateTimeField
    val dimension = Dimension("dim1", "eventKey", "identity", defaultDimension)
    val timeDimension = aggregation.cube.Dimension("minute", "minute", "minute", timeField)
    val initSchema = StructType(Seq(
      StructField("eventKey", StringType, false),
      StructField("minute", TimestampType, false)
    ))
    val cube = Cube(name,
      Seq(dimension, timeDimension),
      Seq(operator),
      initSchema,
      initSchema,
      TypeOp.Timestamp,
      expiringDataConfig = None,
      Seq.empty[Trigger],
      WriterOptions()
    )
    val dataCube = new CubeOperations(cube)

    testOperation(getEventInput(sqlTimestamp), dataCube.extractDimensionsAggregations,
      getEventOutput(sqlTimestamp, millis), PreserverOrder)
  }

  /**
   * It gets a stream of data to test.
   * @return a stream of data.
   */
  def getEventInput(ts: Timestamp): Seq[Seq[Row]] =
    Seq(Seq(
      Row("value1", ts),
      Row("value2", ts),
      Row("value3", ts)
    ))

  /**
   * The expected result to test the DataCube output.
   * @return the expected result to test
   */
  def getEventOutput(timestamp: Timestamp, millis: Long):
  Seq[Seq[(DimensionValuesTime, InputFields)]] = {
    val dimensionString = cube.Dimension("dim1", "eventKey", "identity", new DefaultField)
    val dimensionTime = cube.Dimension("minute", "minute", "minute", new DateTimeField)
    val dimensionValueString1 = DimensionValue(dimensionString, "value1")
    val dimensionValueString2 = dimensionValueString1.copy(value = "value2")
    val dimensionValueString3 = dimensionValueString1.copy(value = "value3")
    val dimensionValueTs = DimensionValue(dimensionTime, timestamp)
    val tsMap = Row(timestamp)
    val valuesMap1 = InputFields(Row("value1", timestamp), 1)
    val valuesMap2 = InputFields(Row("value2", timestamp), 1)
    val valuesMap3 = InputFields(Row("value3", timestamp), 1)

    Seq(Seq(
      (DimensionValuesTime("cubeName", Seq(dimensionValueString1, dimensionValueTs)), valuesMap1),
      (DimensionValuesTime("cubeName", Seq(dimensionValueString2, dimensionValueTs)), valuesMap2),
      (DimensionValuesTime("cubeName", Seq(dimensionValueString3, dimensionValueTs)), valuesMap3)
    ))
  }
}
