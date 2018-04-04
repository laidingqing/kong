
package com.kong.eos.sdk.pipeline.schema

import com.kong.eos.sdk.pipeline.aggregation.cube.Precision
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class TypeConversionsTest extends WordSpec with Matchers {

  "TypeConversions" should {

    val typeConvesions = new TypeConversionsMock

    "typeOperation must be " in {
      val expected = TypeOp.Int
      val result = typeConvesions.defaultTypeOperation
      result should be(expected)
    }

    "operationProps must be " in {
      val expected = Map("typeOp" -> "string")
      val result = typeConvesions.operationProps
      result should be(expected)
    }

    "the operation type must be " in {
      val expected = Some(TypeOp.String)
      val result = typeConvesions.getTypeOperation
      result should be(expected)
    }

    "the detailed operation type must be " in {
      val expected = Some(TypeOp.String)
      val result = typeConvesions.getTypeOperation("string")
      result should be(expected)
    }

    "the precision type must be " in {
      val expected = Precision("precision", TypeOp.String, Map())
      val result = typeConvesions.getPrecision("precision", Some(TypeOp.String))
      result should be(expected)
    }
  }
}
