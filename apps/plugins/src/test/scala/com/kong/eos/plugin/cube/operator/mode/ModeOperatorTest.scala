
package com.kong.eos.plugin.cube.operator.mode

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class ModeOperatorTest extends WordSpec with Matchers {

  "Mode operator" should {

    val initSchema = StructType(Seq(
      StructField("field1", IntegerType, false),
      StructField("field2", IntegerType, false),
      StructField("field3", IntegerType, false)
    ))

    val initSchemaFail = StructType(Seq(
      StructField("field2", IntegerType, false)
    ))

    "processMap must be " in {
      val inputField = new ModeOperator("mode", initSchema, Map())
      inputField.processMap(Row(1, 2)) should be(None)

      val inputFields2 = new ModeOperator("mode", initSchemaFail, Map("inputField" -> "field1"))
      inputFields2.processMap(Row(1, 2)) should be(None)

      val inputFields3 = new ModeOperator("mode", initSchema, Map("inputField" -> "field1"))
      inputFields3.processMap(Row(1, 2)) should be(Some(1))

      val inputFields4 = new ModeOperator("mode", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \"<\", \"value\":2}]"))
      inputFields4.processMap(Row(1, 2)) should be(Some(1L))

      val inputFields5 = new ModeOperator("mode", initSchema,
        Map("inputField" -> "field1", "filters" -> "[{\"field\":\"field1\", \"type\": \">\", \"value\":\"2\"}]"))
      inputFields5.processMap(Row(1, 2)) should be(None)

      val inputFields6 = new ModeOperator("mode", initSchema,
        Map("inputField" -> "field1", "filters" -> {
          "[{\"field\":\"field1\", \"type\": \"<\", \"value\":\"2\"}," +
            "{\"field\":\"field2\", \"type\": \"<\", \"value\":\"2\"}]"
        }))
      inputFields6.processMap(Row(1, 2)) should be(None)
    }

    "processReduce must be " in {
      val inputFields = new ModeOperator("mode", initSchema, Map())
      inputFields.processReduce(Seq()) should be(Some(List()))

      val inputFields2 = new ModeOperator("mode", initSchema, Map())
      inputFields2.processReduce(Seq(Some("hey"), Some("hey"), Some("hi"))) should be(Some(List("hey")))

      val inputFields3 = new ModeOperator("mode", initSchema, Map())
      inputFields3.processReduce(Seq(Some("1"), Some("1"), Some("4"))) should be(Some(List("1")))

      val inputFields4 = new ModeOperator("mode", initSchema, Map())
      inputFields4.processReduce(Seq(
        Some("1"), Some("1"), Some("4"), Some("4"), Some("4"), Some("4"))) should be(Some(List("4")))

      val inputFields5 = new ModeOperator("mode", initSchema, Map())
      inputFields5.processReduce(Seq(
        Some("1"), Some("1"), Some("2"), Some("2"), Some("4"), Some("4"))) should be(Some(List("1", "2", "4")))

      val inputFields6 = new ModeOperator("mode", initSchema, Map())
      inputFields6.processReduce(Seq(
        Some("1"), Some("1"), Some("2"), Some("2"), Some("4"), Some("4"), Some("5"))
      ) should be(Some(List("1", "2", "4")))
    }
  }
}
