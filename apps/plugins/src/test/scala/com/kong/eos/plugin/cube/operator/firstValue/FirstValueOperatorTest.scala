
package com.kong.eos.plugin.cube.operator.firstValue

import java.util.Date

import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}


@RunWith(classOf[JUnitRunner])
class FirstValueOperatorTest extends WordSpec with Matchers {

  "FirstValue operator" should {

    val initSchema = StructType(Seq(
      StructField("field1", IntegerType, false),
      StructField("field2", IntegerType, false),
      StructField("field3", IntegerType, false)
    ))

    val initSchemaFail = StructType(Seq(
      StructField("field2", IntegerType, false)
    ))

    "processMap must be " in {
      val inputField = new FirstValueOperator("firstValue", initSchema, Map())
      inputField.processMap(Row(1, 2)) should be(None)

      val inputFields2 = new FirstValueOperator("firstValue", initSchemaFail, Map("inputField" -> "field1"))
      inputFields2.processMap(Row(1, 2)) should be(None)

      val inputFields3 = new FirstValueOperator("firstValue", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row(1, 2)) should be(Some(1))

      val inputFields4 = new FirstValueOperator("firstValue", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \"<\", \"value\":2}]"))
      inputFields4.processMap(Row(1, 2)) should be(Some(1L))

      val inputFields5 = new FirstValueOperator("firstValue", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \">\", \"value\":\"2\"}]"))
      inputFields5.processMap(Row(1, 2)) should be(None)

      val inputFields6 = new FirstValueOperator("firstValue", initSchema,
        Map("inputField" -> "field1", "filters" -> {
          "[{\"field\":\"field1\", \"type\": \"<\", \"value\":\"2\"}," +
            "{\"field\":\"field2\", \"type\": \"<\", \"value\":\"2\"}]"
        }))
      inputFields6.processMap(Row(1, 2)) should be(None)
    }

    "processReduce must be " in {
      val inputFields = new FirstValueOperator("firstValue", initSchema, Map())
      inputFields.processReduce(Seq()) should be(None)

      val inputFields2 = new FirstValueOperator("firstValue", initSchema, Map())
      inputFields2.processReduce(Seq(Some(1), Some(2))) should be(Some(1))

      val inputFields3 = new FirstValueOperator("firstValue", initSchema, Map())
      inputFields3.processReduce(Seq(Some("a"), Some("b"))) should be(Some("a"))
    }

    "associative process must be " in {
      val inputFields = new FirstValueOperator("firstValue", initSchema, Map())
      val resultInput = Seq((Operator.OldValuesKey, Some(1L)),
        (Operator.NewValuesKey, Some(1L)),
        (Operator.NewValuesKey, None))
      inputFields.associativity(resultInput) should be(Some(1L))

      val inputFields2 = new FirstValueOperator("firstValue", initSchema, Map("typeOp" -> "int"))
      val resultInput2 = Seq((Operator.OldValuesKey, Some(1L)),
        (Operator.NewValuesKey, Some(1L)))
      inputFields2.associativity(resultInput2) should be(Some(1))

      val inputFields3 = new FirstValueOperator("firstValue", initSchema, Map("typeOp" -> null))
      val resultInput3 = Seq((Operator.OldValuesKey, Some(1)),
        (Operator.NewValuesKey, Some(1)),
        (Operator.NewValuesKey, None))
      inputFields3.associativity(resultInput3) should be(Some(1))

      val inputFields4 = new FirstValueOperator("firstValue", initSchema, Map())
      val resultInput4 = Seq()
      inputFields4.associativity(resultInput4) should be(None)

      val inputFields5 = new FirstValueOperator("firstValue", initSchema, Map())
      val date = new Date()
      val resultInput5 = Seq((Operator.NewValuesKey, Some(date)))
      inputFields5.associativity(resultInput5) should be(Some(date))
    }
  }
}
