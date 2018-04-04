
package com.kong.eos.plugin.transformation.morphline

import java.io.Serializable

import com.kong.eos.sdk.pipeline.input.Input
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpecLike}



@RunWith(classOf[JUnitRunner])
class MorphlinesParserTest extends WordSpecLike with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  val morphlineConfig = """
          id : test1
          importCommands : ["org.kitesdk.**"]
          commands: [
          {
              readJson {},
          }
          {
              extractJsonPaths {
                  paths : {
                      col1 : /col1
                      col2 : /col2
                  }
              }
          }
          {
            java {
              code : "return child.process(record);"
            }
          }
          {
              removeFields {
                  blacklist:["literal:_attachment_body"]
              }
          }
          ]
                        """
  val inputField = Some(Input.RawDataKey)
  val outputsFields = Seq("col1", "col2")
  val props: Map[String, Serializable] = Map("morphline" -> morphlineConfig)

  val schema = StructType(Seq(StructField("col1", StringType), StructField("col2", StringType)))

  val parser = new MorphlinesParser(1, inputField, outputsFields, schema, props)

  "A MorphlinesParser" should {

    "parse a simple json" in {
      val simpleJson =
        """{
            "col1":"hello",
            "col2":"word"
            }
        """
      val input = Row(simpleJson)
      val result = parser.parse(input)

      val expected = Seq(Row(simpleJson, "hello", "world"))

      result should be eq(expected)
    }

    "parse a simple json removing raw" in {
      val simpleJson =
        """{
            "col1":"hello",
            "col2":"word"
            }
        """
      val input = Row(simpleJson)
      val result = parser.parse(input)

      val expected = Seq(Row("hello", "world"))

      result should be eq(expected)
    }

    "exclude not configured fields" in {
      val simpleJson =
        """{
            "col1":"hello",
            "col2":"word",
            "col3":"!"
            }
        """
      val input = Row(simpleJson)
      val result = parser.parse(input)

      val expected = Seq(Row(simpleJson, "hello", "world"))

      result should be eq(expected)
    }
  }
}
