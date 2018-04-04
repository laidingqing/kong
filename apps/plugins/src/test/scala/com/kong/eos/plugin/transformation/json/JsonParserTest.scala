
package com.kong.eos.plugin.transformation.json

import java.io.{Serializable => JSerializable}

import com.jayway.jsonpath.PathNotFoundException
import com.kong.eos.sdk.pipeline.transformation.WhenError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}


@RunWith(classOf[JUnitRunner])
class JsonParserTest extends WordSpecLike with Matchers {

  val inputField = Some("json")
  val schema = StructType(Seq(
    StructField(inputField.get, StringType),
    StructField("color", StringType),
    StructField("price", DoubleType))
  )
  val JSON = """{ "store": {
               |    "book": [
               |      { "category": "reference",
               |        "author": "Nigel Rees",
               |        "title": "Sayings of the Century",
               |        "price": 8.95
               |      },
               |      { "category": "fiction",
               |        "author": "Evelyn Waugh",
               |        "title": "Sword of Honour",
               |        "price": 12.99
               |      },
               |      { "category": "fiction",
               |        "author": "Herman Melville",
               |        "title": "Moby Dick",
               |        "isbn": "0-553-21311-3",
               |        "price": 8.99
               |      },
               |      { "category": "fiction",
               |        "author": "J. R. R. Tolkien",
               |        "title": "The Lord of the Rings",
               |        "isbn": "0-395-19395-8",
               |        "price": 22.99
               |      }
               |    ],
               |    "bicycle": {
               |      "color": "red",
               |      "price": 19.95
               |    }
               |  }
               |}""".stripMargin

  //scalastyle:off
  "A JsonParser" should {

    "parse json string" in {
      val input = Row(JSON)
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin
      val result = new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row(JSON, "red", 19.95))

      assertResult(result)(expected)
    }

    "parse json string removing raw" in {
      val input = Row(JSON)
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin
      val result = new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable], "removeInputField" -> "true")
      ).parse(input)
      val expected = Seq(Row("red", 19.95))

      assertResult(result)(expected)
    }

    "not parse anything if the field does not match" in {
      val input = Row(JSON)
      val schema = StructType(Seq(StructField("wrongfield", StringType)))
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin

      an[IllegalStateException] should be thrownBy new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
    }

    "not parse when input is wrong" in {
      val input = Row("{}")
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin

      an[Exception] should be thrownBy new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
    }

    "parse when input is null" in {
      val JSON = """{ "store": {
                   |    "bicycle": {
                   |      "color": "red",
                   |      "price": null
                   |    }
                   |  }
                   |}""".stripMargin

      val input = Row(JSON)
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin
      val result = new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable], "whenError" -> WhenError.Null)
      ).parse(input)
      val expected = Seq(Row(JSON, "red", null))

      assertResult(result)(expected)
    }

    "parse when input is not found " in {
      val JSON = """{ "store": {
                   |    "bicycle": {
                   |      "color": "red"
                   |    }
                   |  }
                   |}""".stripMargin

      val input = Row(JSON)
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin
      val result = new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable], "whenError" -> WhenError.Null)
      ).parse(input)
      val expected = Seq(Row(JSON, "red", null))

      assertResult(result)(expected)
    }

    "parse when input is not found and return is error or discard" in {
      val JSON = """{ "store": {
                   |    "bicycle": {
                   |      "color": "red"
                   |    }
                   |  }
                   |}""".stripMargin

      val input = Row(JSON)
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"color",
          |   "query":"$.store.bicycle.color"
          |},
          |{
          |   "field":"price",
          |   "query":"$.store.bicycle.price"
          |}]
          |""".stripMargin
      an[PathNotFoundException] should be thrownBy new JsonParser(
        1,
        inputField,
        outputsFields,
        schema,
        Map("queries" -> queries.asInstanceOf[JSerializable], "whenError" -> WhenError.Error)
      ).parse(input)
    }
  }
}