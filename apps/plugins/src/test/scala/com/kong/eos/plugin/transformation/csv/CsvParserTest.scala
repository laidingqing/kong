
package com.kong.eos.plugin.transformation.csv

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.properties.JsoneyString
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}


@RunWith(classOf[JUnitRunner])
class CsvParserTest extends WordSpecLike with Matchers {

  val inputField = Some("csv")
  val schema = StructType(Seq(
    StructField(inputField.get, StringType),
    StructField("color", StringType),
    StructField("price", DoubleType))
  )
  val CSV = "red,19.95"

  //scalastyle:off
  "A CsvParser" should {

    "parse CSV string" in {
      val input = Row(CSV)
      val outputsFields = Seq("color", "price")
      val fields =
        """[
          |{
          |   "name":"color"
          |},
          |{
          |   "name":"price"
          |}]
          |""".stripMargin
      val result = new CsvParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("fields" -> fields.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row(CSV, "red", 19.95))

      assertResult(result)(expected)
    }

    "parse CSV string removing raw" in {
      val input = Row(CSV)
      val outputsFields = Seq("color", "price")
      val fields =
        """[
          |{
          |   "name":"color"
          |},
          |{
          |   "name":"price"
          |}]
          |""".stripMargin
      val result = new CsvParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("fields" -> fields.asInstanceOf[JSerializable], "removeInputField" -> JsoneyString.apply("true"))
      ).parse(input)
      val expected = Seq(Row("red", 19.95))

      assertResult(result)(expected)
    }

    "not parse anything if the field does not match" in {
      val input = Row(CSV)
      val schema = StructType(Seq(StructField("wrongfield", StringType)))
      val outputsFields = Seq("color", "price")
      val fields =
        """[
          |{
          |   "name":"color"
          |},
          |{
          |   "name":"price"
          |}]
          |""".stripMargin

      an[IllegalStateException] should be thrownBy new CsvParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("fields" -> fields.asInstanceOf[JSerializable])
      ).parse(input)
    }

    "not parse when input is wrong" in {
      val input = Row("{}")
      val outputsFields = Seq("color", "price")
      val fields =
        """[
          |{
          |   "name":"color"
          |},
          |{
          |   "name":"price"
          |}]
          |""".stripMargin

      an[Exception] should be thrownBy new CsvParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("fields" -> fields.asInstanceOf[JSerializable])
      ).parse(input)
    }
  }
}