
package com.kong.eos.plugin.transformation.json

import com.jayway.jsonpath.PathNotFoundException
import org.scalatest._
import org.scalatest.mock.MockitoSugar

class JsonPathExtractorTest extends FlatSpec with ShouldMatchers with MockitoSugar {

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


  it should "return bicycle color with dot-notation query" in {
    val query = "$.store.bicycle.color"

    val result = new JsonPathExtractor(JSON, false).query(query)

    result.asInstanceOf[String] should be("red")
  }

  it should "return bicycle color with bracket-notation query" in {
    val query = "$['store']['bicycle']['color']"

    val result = new JsonPathExtractor(JSON, false).query(query)

    result.asInstanceOf[String] should be("red")
  }

  it should "return bicycle price" in {
    val query = "$.store.bicycle.price"

    val result = new JsonPathExtractor(JSON, false).query(query)

    result.asInstanceOf[Double] should be(19.95)
  }

  it should "return null with leaf" in {
    val query = "$.store.bicycle.bad"

    val result = new JsonPathExtractor(JSON, true).query(query)

    result.asInstanceOf[String] should be(null)
  }

  it should "return exception without leaf" in {
    val query = "$.store.bicycle.bad"

    an[PathNotFoundException] should be thrownBy new JsonPathExtractor(JSON, false).query(query)

  }
}
