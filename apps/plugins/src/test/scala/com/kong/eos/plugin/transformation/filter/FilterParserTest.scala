
package com.kong.eos.plugin.transformation.filter

import java.io.{Serializable => JSerializable}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}



@RunWith(classOf[JUnitRunner])
class FilterParserTest extends WordSpecLike with Matchers {

  val schema = StructType(Seq(
    StructField("color", StringType),
    StructField("price", DoubleType))
  )
  val whiteValues = Seq("white", 5.0)
  val whiteRow = Row.fromSeq(whiteValues)
  val blackRow = Row.fromSeq(Seq("black", 5.0))
  val tenRow = Row.fromSeq(Seq("white", 10.0))


  //scalastyle:off
  "A FilterParser" should {

    "parse filter string adding output fields" in {
      val input = whiteRow
      val outputsFields = Seq()
      val queries =
        """[
          |{
          |   "field":"color",
          |   "type":"=",
          |   "value":"white"
          |}
          |]
          |""".stripMargin

      val result = new FilterParser(
        1,
        None,
        outputsFields,
        schema,
        Map("filters" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row.fromSeq(whiteValues))

      assertResult(result)(expected)
    }

    "parse filter string removing output fields" in {
      val input = whiteRow
      val outputsFields = Seq()
      val queries =
        """[
          |{
          |   "field":"color",
          |   "type":"=",
          |   "value":"white"
          |}
          |]
          |""".stripMargin

      val result = new FilterParser(
        1,
        None,
        outputsFields,
        schema,
        Map("filters" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row.fromSeq(whiteValues))

      assertResult(result)(expected)
    }

    "parse filter double adding output fields" in {
      val input = whiteRow
      val outputsFields = Seq()
      val queries =
        """[
          |{
          |   "field":"price",
          |   "type":">",
          |   "value":4.0
          |}
          |]
          |""".stripMargin

      val result = new FilterParser(
        1,
        None,
        outputsFields,
        schema,
        Map("filters" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row.fromSeq(whiteValues))

      assertResult(result)(expected)
    }

    "parse filter double discarding row" in {
      val input = whiteRow
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"price",
          |   "type":">",
          |   "value":6.0
          |}
          |]
          |""".stripMargin

      val result = new FilterParser(
        1,
        None,
        outputsFields,
        schema,
        Map("filters" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq.empty

      assertResult(result)(expected)
    }

    "parse filter discarding row with two filters" in {
      val input = whiteRow
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"price",
          |   "type":">",
          |   "value":6.0
          |},
          |{
          |   "field":"color",
          |   "type":"=",
          |   "value":"white"
          |}
          |]
          |""".stripMargin

      val result = new FilterParser(
        1,
        None,
        outputsFields,
        schema,
        Map("filters" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq.empty

      assertResult(result)(expected)
    }

    "parse filter row with two filters" in {
      val input = whiteRow
      val outputsFields = Seq("color", "price")
      val queries =
        """[
          |{
          |   "field":"price",
          |   "type":"<",
          |   "value":6.0
          |},
          |{
          |   "field":"color",
          |   "type":"!=",
          |   "value":"black"
          |}
          |]
          |""".stripMargin

      val result = new FilterParser(
        1,
        None,
        outputsFields,
        schema,
        Map("filters" -> queries.asInstanceOf[JSerializable])
      ).parse(input)
      val expected = Seq(Row.fromSeq(whiteValues))

      assertResult(result)(expected)
    }
  }
}