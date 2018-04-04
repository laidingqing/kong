
package com.kong.eos.plugin.cube.operator.stddev

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}


@RunWith(classOf[JUnitRunner])
class StddevOperatorTest extends WordSpec with Matchers {

  "Std dev operator" should {

    val initSchema = StructType(Seq(
      StructField("field1", IntegerType, false),
      StructField("field2", IntegerType, false),
      StructField("field3", IntegerType, false)
    ))

    val initSchemaFail = StructType(Seq(
      StructField("field2", IntegerType, false)
    ))

    "processMap must be " in {
      val inputField = new StddevOperator("stdev", initSchema, Map())
      inputField.processMap(Row(1, 2)) should be(None)

      val inputFields2 = new StddevOperator("stdev", initSchemaFail, Map("inputField" -> "field1"))
      inputFields2.processMap(Row(1, 2)) should be(None)

      val inputFields3 = new StddevOperator("stdev", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row(1, 2)) should be(Some(1))

      val inputFields4 = new StddevOperator("stdev", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row("1", 2)) should be(Some(1))

      val inputFields6 = new StddevOperator("stdev", initSchema, Map("inputField" -> "field1"))
      inputFields6.processMap(Row(1.5, 2)) should be(Some(1.5))

      val inputFields7 = new StddevOperator("stdev", initSchema, Map("inputField" -> "field1"))
      inputFields7.processMap(Row(5L, 2)) should be(Some(5L))

      val inputFields8 = new StddevOperator("stdev", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \"<\", \"value\":2}]"))
      inputFields8.processMap(Row(1, 2)) should be(Some(1L))

      val inputFields9 = new StddevOperator("stdev", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \">\", \"value\":\"2\"}]"))
      inputFields9.processMap(Row(1, 2)) should be(None)

      val inputFields10 = new StddevOperator("stdev", initSchema,
        Map("inputField" -> "field1", "filters" -> {
          "[{\"field\":\"field1\", \"type\": \"<\", \"value\":\"2\"}," +
            "{\"field\":\"field2\", \"type\": \"<\", \"value\":\"2\"}]"
        }))
      inputFields10.processMap(Row(1, 2)) should be(None)
    }

    "processReduce must be " in {
      val inputFields = new StddevOperator("stdev", initSchema, Map())
      inputFields.processReduce(Seq()) should be(Some(0d))

      val inputFields2 = new StddevOperator("stdev", initSchema, Map())
      inputFields2.processReduce(Seq(Some(1), Some(2), Some(3), Some(7), Some(7))) should be
      (Some(2.8284271247461903))

      val inputFields3 = new StddevOperator("stdev", initSchema, Map())
      inputFields3.processReduce(Seq(Some(1), Some(2), Some(3), Some(6.5), Some(7.5))) should be
      (Some(2.850438562747845))

      val inputFields4 = new StddevOperator("stdev", initSchema, Map())
      inputFields4.processReduce(Seq(None)) should be(Some(0d))

      val inputFields5 = new StddevOperator("stdev", initSchema, Map("typeOp" -> "string"))
      inputFields5.processReduce(
        Seq(Some(1), Some(2), Some(3), Some(6.5), Some(7.5))) should be(Some("2.850438562747845"))
    }

    "processReduce distinct must be " in {
      val inputFields = new StddevOperator("stdev", initSchema, Map("distinct" -> "true"))
      inputFields.processReduce(Seq()) should be(Some(0d))

      val inputFields2 = new StddevOperator("stdev", initSchema, Map("distinct" -> "true"))
      inputFields2.processReduce(Seq(Some(1), Some(2), Some(3), Some(7), Some(7))) should be
      (Some(2.8284271247461903))

      val inputFields3 = new StddevOperator("stdev", initSchema, Map("distinct" -> "true"))
      inputFields3.processReduce(Seq(Some(1), Some(1), Some(2), Some(3), Some(6.5), Some(7.5))) should be
      (Some(2.850438562747845))

      val inputFields4 = new StddevOperator("stdev", initSchema, Map("distinct" -> "true"))
      inputFields4.processReduce(Seq(None)) should be(Some(0d))

      val inputFields5 = new StddevOperator("stdev", initSchema, Map("typeOp" -> "string", "distinct" -> "true"))
      inputFields5.processReduce(
        Seq(Some(1), Some(1), Some(2), Some(3), Some(6.5), Some(7.5))) should be(Some("2.850438562747845"))
    }
  }
}
