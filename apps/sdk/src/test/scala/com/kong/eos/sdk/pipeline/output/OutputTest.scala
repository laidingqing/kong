package com.kong.eos.sdk.pipeline.output

import com.kong.eos.sdk.pipeline.aggregation.cube.{Dimension, DimensionValue}
import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionTypeMock, DimensionValue, DimensionValuesTime}
import com.kong.eos.sdk.pipeline.transformation.OutputMock
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class OutputTest extends WordSpec with Matchers {

  trait CommonValues {
    val timeDimension = "minute"
    val tableName = "table"
    val timestamp = 1L
    val defaultDimension = new DimensionTypeMock(Map())
    val dimensionValuesT = DimensionValuesTime("testCube", Seq(
      DimensionValue(Dimension("dim1", "eventKey", "identity", defaultDimension), "value1"),
      DimensionValue(Dimension("dim2", "eventKey", "identity", defaultDimension), "value2"),
      DimensionValue(Dimension("minute", "eventKey", "identity", defaultDimension), 1L)))
    val dimensionValuesTFixed = DimensionValuesTime("testCube", Seq(
      DimensionValue(Dimension("dim1", "eventKey", "identity", defaultDimension), "value1"),
      DimensionValue(Dimension("minute", "eventKey", "identity", defaultDimension), 1L)))
    val outputName = "outputName"
    val output = new OutputMock(outputName, Map())
    val outputOperation = new OutputMock(outputName, Map())
    val outputProps = new OutputMock(outputName, Map())
  }

  "Output" should {

    "Name must be " in new CommonValues {
      val expected = outputName
      val result = output.name
      result should be(expected)
    }

    "the spark geo field returned must be " in new CommonValues {
      val expected = StructField("field", ArrayType(DoubleType), false)
      val result = Output.defaultGeoField("field", false)
      result should be(expected)
    }

    "classSuffix must be " in {
      val expected = "Output"
      val result = Output.ClassSuffix
      result should be(expected)
    }
  }
}
