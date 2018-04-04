
package com.kong.eos.plugin.cube.operator.accumulator

import com.kong.eos.sdk.pipeline.aggregation.operator.Operator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
@RunWith(classOf[JUnitRunner])
class AccumulatorOperatorTest extends WordSpec with Matchers {

  "Accumulator operator" should {

    val initSchema = StructType(Seq(
      StructField("field1", IntegerType, false),
      StructField("field2", IntegerType, false)
    ))

    val initSchemaFail = StructType(Seq(
      StructField("field2", IntegerType, false)
    ))

    "processMap must be " in {
      val inputField = new AccumulatorOperator("accumulator", initSchema, Map())
      inputField.processMap(Row(1, 2)) should be(None)

      val inputFields2 = new AccumulatorOperator("accumulator", initSchemaFail, Map("inputField" -> "field1"))
      inputFields2.processMap(Row(1, 2)) should be(None)

      val inputFields3 = new AccumulatorOperator("accumulator", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row(1, 2)) should be(Some(1))

      val inputFields4 = new AccumulatorOperator("accumulator", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \"<\", \"value\":2}]"))
      inputFields4.processMap(Row(1, 2)) should be(Some(1L))

      val inputFields5 = new AccumulatorOperator("accumulator", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \">\", \"value\":2}]"))
      inputFields5.processMap(Row(1, 2)) should be(None)

      val inputFields6 = new AccumulatorOperator("accumulator", initSchema,
        Map("inputField" -> "field1", "filters" -> {
          "[{\"field\":\"field1\", \"type\": \"<\", \"value\":2}," +
            "{\"field\":\"field2\", \"type\": \"<\", \"value\":2}]"
        }))
      inputFields6.processMap(Row(1, 2)) should be(None)
    }

    "processReduce must be " in {
      val inputFields = new AccumulatorOperator("accumulator", initSchema, Map())
      inputFields.processReduce(Seq()) should be(Some(Seq()))

      val inputFields2 = new AccumulatorOperator("accumulator", initSchema, Map())
      inputFields2.processReduce(Seq(Some(1), Some(1))) should be(Some(Seq("1", "1")))

      val inputFields3 = new AccumulatorOperator("accumulator", initSchema, Map())
      inputFields3.processReduce(Seq(Some("a"), Some("b"))) should be(Some(Seq("a", "b")))
    }

    "associative process must be " in {
      val inputFields = new AccumulatorOperator("accumulator", initSchema, Map())
      val resultInput = Seq((Operator.OldValuesKey, Some(Seq(1L))),
        (Operator.NewValuesKey, Some(Seq(2L))),
        (Operator.NewValuesKey, None))
      inputFields.associativity(resultInput) should be(Some(Seq("1", "2")))

      val inputFields2 = new AccumulatorOperator("accumulator", initSchema, Map("typeOp" -> "arraydouble"))
      val resultInput2 = Seq((Operator.OldValuesKey, Some(Seq(1))),
        (Operator.NewValuesKey, Some(Seq(3))))
      inputFields2.associativity(resultInput2) should be(Some(Seq(1d, 3d)))

      val inputFields3 = new AccumulatorOperator("accumulator", initSchema, Map("typeOp" -> null))
      val resultInput3 = Seq((Operator.OldValuesKey, Some(Seq(1))),
        (Operator.NewValuesKey, Some(Seq(1))))
      inputFields3.associativity(resultInput3) should be(Some(Seq("1", "1")))
    }
  }
}
