
package com.kong.eos.plugin.transformation.geo

import java.io.{Serializable => JSerializable}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}


@RunWith(classOf[JUnitRunner])
class GeoParserTest extends WordSpecLike with Matchers {

  trait GeoParserUnitTestComponent {

    val latitudeField = "lat"
    val longitudeField = "long"
    val latitudeValue = "12.1231"
    val longitudeValue = "13.1231"
    val outputField = "geo"

    val preRow: Row
    val postRow: Seq[Row]
    val schema: StructType
    val properties: Map[String, JSerializable]

    lazy val resultantRow = new GeoParser(1, None, Seq(outputField), schema, properties)
      .parse(preRow)
  }

  "A GeoParser" when {
    " when parse the geo position" should {
      "return the latitude field if it's defined in the properties map" in new GeoParserUnitTestComponent {

        val data = Seq(latitudeValue, longitudeValue)
        val schema = StructType(Seq(StructField(latitudeField, DoubleType),
          StructField("longitude", DoubleType),
          StructField("geo", StringType))
        )
        val properties: Map[String, JSerializable] = Map("latitude" -> latitudeField)

        val preRow = Row.fromSeq(data)
        val postRow = Seq(Row.fromSeq(data ++ Seq(s"${latitudeValue}__$longitudeValue")))

        postRow should be(resultantRow)
      }

      "return the longitude field if it's defined in the properties map" in new GeoParserUnitTestComponent {

        val data = Seq(latitudeValue, longitudeValue)
        val schema = StructType(Seq(StructField("latitude", DoubleType),
          StructField(longitudeField, DoubleType),
          StructField("geo", StringType))
        )
        val properties = Map("longitude" -> longitudeField)

        val preRow = Row.fromSeq(data)
        val postRow = Seq(Row.fromSeq(data ++ Seq(s"${latitudeValue}__$longitudeValue")))

        postRow should be(resultantRow)
      }

      "return the default latitude field if it's not defined in the properties map" in new GeoParserUnitTestComponent {

        val data = Seq(latitudeValue, longitudeValue)
        val schema = StructType(Seq(StructField("latitude", DoubleType),
          StructField("longitude", DoubleType),
          StructField("geo", StringType))
        )
        val properties = Map.empty[String, Serializable]

        val preRow = Row.fromSeq(data)
        val postRow = Seq(Row.fromSeq(data ++ Seq(s"${latitudeValue}__$longitudeValue")))

        postRow should be(resultantRow)
      }

      "return the default longitude field if it's not defined in the properties map" in new GeoParserUnitTestComponent {

        val data = Seq(latitudeValue, longitudeValue)
        val schema = StructType(Seq(StructField("latitude", DoubleType),
          StructField("longitude", DoubleType),
          StructField("geo", StringType))
        )
        val properties = Map.empty[String, Serializable]

        val preRow = Row.fromSeq(data)
        val postRow = Seq(Row.fromSeq(data ++ Seq(s"${latitudeValue}__$longitudeValue")))

        postRow should be(resultantRow)
      }

      "return one exception if there isn't a latitude field" in new GeoParserUnitTestComponent {

        val data = Seq(longitudeValue)
        val schema = StructType(Seq(StructField("latitude", DoubleType),
          StructField("geo", StringType))
        )
        val properties = Map("latitude" -> latitudeField)
        val preRow = Row.fromSeq(data)
        val postRow = Seq(preRow)

        an[RuntimeException] should be thrownBy be(resultantRow)
      }

      "return one exception if there isn't a longitude field" in new GeoParserUnitTestComponent {

        val data = Seq(latitudeValue)
        val schema = StructType(Seq(StructField("longitude", DoubleType),
          StructField("geo", StringType))
        )
        val properties = Map("longitude" -> longitudeField)
        val preRow = Row.fromSeq(data)
        val postRow = Seq(preRow)

        an[RuntimeException] should be thrownBy be(resultantRow)
      }

      "return one exception if there isn't a latitude field (default value)" in new GeoParserUnitTestComponent {

        val data = Seq(longitudeValue)
        val schema = StructType(Seq(StructField("longitude", DoubleType), StructField("geo", StringType)))
        val properties = Map.empty[String, Serializable]
        val preRow = Row.fromSeq(data)
        val postRow = Seq(preRow)

        an[RuntimeException] should be thrownBy be(resultantRow)
      }

      "return one exception if there isn't a longitude field (default value)" in new GeoParserUnitTestComponent {

        val data = Seq(latitudeValue)
        val schema = StructType(Seq(StructField("latitude", DoubleType), StructField("geo", StringType)))
        val properties = Map.empty[String, Serializable]
        val preRow = Row.fromSeq(data)
        val postRow = Seq(preRow)

        an[RuntimeException] should be thrownBy be(resultantRow)
      }
    }
  }
}
