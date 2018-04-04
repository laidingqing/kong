
package com.kong.eos.plugin.transformation.xpath

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.properties.JsoneyString
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.scalatest._


class XPathParserTest extends WordSpecLike with Matchers {


  val inputField = Some("xml")
  val schema = StructType(Seq(
    StructField(inputField.get, StringType),
    StructField("id", StringType),
    StructField("enabled", BooleanType))
  )
  val XML = """
               |<root>
               |    <element id="1" enabled="true"/>
               |    <element id="2" enabled="false"/>
               |    <element id="3" enabled="true"/>
               |    <element id="4" enabled="false"/>
               |</root>
             """.stripMargin

  //scalastyle:off
  
  
  "A XPathParser" should {

    "parse xml string" in {
      val input = Row(XML)
      val outputsFields = Seq("id", "enabled")
      val queries =
        """[
          |{
          |   "field":"id",
          |   "query":"//element/@id"
          |},
          |{
          |   "field":"enabled",
          |   "query":"//element/@enabled"
          |}]
          |""".stripMargin

      val result = new XPathParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row(XML, "1", true))

      assertResult(result)(expected)
    }

    "parse xml string removing raw" in {
      val input = Row(XML)
      val outputsFields = Seq("id", "enabled")
      val queries =
        """[
          |{
          |   "field":"id",
          |   "query":"//element/@id"
          |},
          |{
          |   "field":"enabled",
          |   "query":"//element/@enabled"
          |}]
          |""".stripMargin

      val result = new XPathParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable], "removeInputField" -> JsoneyString.apply("true"))
      ).parse(input)

      val expected = Seq(Row("1", true))

      assertResult(result)(expected)
    }

    "not parse anything if the field does not match" in {
      val input = Row(XML)
      val schema = StructType(Seq(StructField("wrongfield", StringType)))
      val outputsFields = Seq("id", "enabled")
      val queries =
        """[
          |{
          |   "field":"id",
          |   "query":"//element/@id"
          |},
          |{
          |   "field":"enabled",
          |   "query":"//element/@enabled"
          |}]
          |""".stripMargin

      an[IllegalStateException] should be thrownBy new XPathParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
    }

    "not parse when input is wrong" in {
      val input = Row("{}")
      val outputsFields = Seq("id", "enabled")
      val queries =
        """[
          |{
          |   "field":"id",
          |   "query":"//element/@id"
          |},
          |{
          |   "field":"enabled",
          |   "query":"//element/@enabled"
          |}]
          |""".stripMargin

      an[Exception] should be thrownBy new XPathParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
    }
  }

}
