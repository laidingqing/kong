
package com.kong.eos.plugin.cube.operator.min

import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class MinOperatorTest extends WordSpec with Matchers {

  "Min operator" should {

    val initSchema = StructType(Seq(
      StructField("field1", IntegerType, false),
      StructField("field2", IntegerType, false),
      StructField("field3", IntegerType, false)
    ))

    val initSchemaFail = StructType(Seq(
      StructField("field2", IntegerType, false)
    ))

    "processMap must be " in {
      val inputField = new MinOperator("min", initSchema, Map())
      inputField.processMap(Row(1, 2)) should be(None)

      val inputFields2 = new MinOperator("min", initSchemaFail, Map("inputField" -> "field1"))
      inputFields2.processMap(Row(1, 2)) should be(None)

      val inputFields3 = new MinOperator("min", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row(1, 2)) should be(Some(1))

      val inputFields4 = new MinOperator("min", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row("1", 2)) should be(Some("1"))

      val inputFields6 = new MinOperator("min", initSchema, Map("inputField" -> "field1"))
      inputFields6.processMap(Row(1.5, 2)) should be(Some(1.5))

      val inputFields7 = new MinOperator("min", initSchema, Map("inputField" -> "field1"))
      inputFields7.processMap(Row(5L, 2)) should be(Some(5L))

      val inputFields8 = new MinOperator("min", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \"<\", \"value\":2}]"))
      inputFields8.processMap(Row(1, 2)) should be(Some(1L))

      val inputFields9 = new MinOperator("min", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \">\", \"value\":\"2\"}]"))
      inputFields9.processMap(Row(1, 2)) should be(None)

      val inputFields10 = new MinOperator("min", initSchema,
        Map("inputField" -> "field1", "filters" -> {
          "[{\"field\":\"field1\", \"type\": \"<\", \"value\":\"2\"}," +
            "{\"field\":\"field2\", \"type\": \"<\", \"value\":\"2\"}]"
        }))
      inputFields10.processMap(Row(1, 2)) should be(None)
    }

    "processReduce must be " in {
      val inputFields = new MinOperator("min", initSchema, Map())
      inputFields.processReduce(Seq()) should be(None)

      val inputFields2 = new MinOperator("min", initSchema, Map())
      inputFields2.processReduce(Seq(Some(1), Some(2))) should be(Some(1d))

      val inputFields3 = new MinOperator("min", initSchema, Map())
      inputFields3.processReduce(Seq(Some(1), Some(2), Some(3))) should be(Some(1d))

      val inputFields4 = new MinOperator("min", initSchema, Map())
      inputFields4.processReduce(Seq(None)) should be(None)
    }

    "processReduce disctinct must be " in {
      val inputFields = new MinOperator("min", initSchema, Map("distinct" -> "true"))
      inputFields.processReduce(Seq()) should be(None)

      val inputFields2 = new MinOperator("min", initSchema, Map("distinct" -> "true"))
      inputFields2.processReduce(Seq(Some(1), Some(2))) should be(Some(1d))

      val inputFields3 = new MinOperator("min", initSchema, Map("distinct" -> "true"))
      inputFields3.processReduce(Seq(Some(1), Some(2), Some(3))) should be(Some(1d))

      val inputFields4 = new MinOperator("min", initSchema, Map("distinct" -> "true"))
      inputFields4.processReduce(Seq(None)) should be(None)
    }

    "associative process must be " in {
      val inputFields = new MinOperator("max", initSchema, Map())
      val resultInput = Seq((Operator.OldValuesKey, Some(1L)),
        (Operator.NewValuesKey, Some(2L)),
        (Operator.NewValuesKey, None))
      inputFields.associativity(resultInput) should be(Some(1d))

      val inputFields2 = new MinOperator("max", initSchema, Map("typeOp" -> "int"))
      val resultInput2 = Seq((Operator.OldValuesKey, Some(1)),
        (Operator.NewValuesKey, Some(2)))
      inputFields2.associativity(resultInput2) should be(Some(1))

      val inputFields3 = new MinOperator("max", initSchema, Map("typeOp" -> null))
      val resultInput3 = Seq((Operator.OldValuesKey, Some(1)),
        (Operator.NewValuesKey, Some(1)))
      inputFields3.associativity(resultInput3) should be(Some(1d))

      val inputFields4 = new MinOperator("max", initSchema, Map("typeOp" -> "string"))
      val resultInput4 = Seq((Operator.OldValuesKey, Some(1)),
        (Operator.NewValuesKey, Some(3)))
      inputFields4.associativity(resultInput4) should be(Some("1"))
    }
  }
}
