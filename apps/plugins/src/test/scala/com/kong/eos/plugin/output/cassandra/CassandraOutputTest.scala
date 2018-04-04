
package com.kong.eos.plugin.output.cassandra

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.properties.JsoneyString
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}


@RunWith(classOf[JUnitRunner])
class CassandraOutputTest extends FlatSpec with Matchers with MockitoSugar with AnswerSugar {

  val s = "sum"
  val properties = Map(("connectionHost", "127.0.0.1"), ("connectionPort", "9042"))

  "getSparkConfiguration" should "return a Seq with the configuration" in {
    val configuration = Map(("connectionHost", "127.0.0.1"), ("connectionPort", "9042"))
    val cass = CassandraOutput.getSparkConfiguration(configuration)

    cass should be(List(("spark.cassandra.connection.host", "127.0.0.1"), ("spark.cassandra.connection.port", "9042")))
  }

  "getSparkConfiguration" should "return all cassandra-spark config" in {
    val config: Map[String, JSerializable] = Map(
      ("sparkProperties" -> JsoneyString(
        "[{\"sparkPropertyKey\":\"spark.cassandra.input.fetch.size_in_rows\",\"sparkPropertyValue\":\"2000\"}," +
          "{\"sparkPropertyKey\":\"spark.cassandra.input.split.size_in_mb\",\"sparkPropertyValue\":\"64\"}]")),
      ("anotherProperty" -> "true")
    )

    val sparkConfig = CassandraOutput.getSparkConfiguration(config)

    sparkConfig.exists(_ == ("spark.cassandra.input.fetch.size_in_rows" -> "2000")) should be(true)
    sparkConfig.exists(_ == ("spark.cassandra.input.split.size_in_mb" -> "64")) should be(true)
    sparkConfig.exists(_ == ("anotherProperty" -> "true")) should be(false)
  }

  "getSparkConfiguration" should "not return cassandra-spark config" in {
    val config: Map[String, JSerializable] = Map(
      ("hadoopProperties" -> JsoneyString(
        "[{\"sparkPropertyKey\":\"spark.cassandra.input.fetch.size_in_rows\",\"sparkPropertyValue\":\"2000\"}," +
          "{\"sparkPropertyKey\":\"spark.cassandra.input.split.size_in_mb\",\"sparkPropertyValue\":\"64\"}]")),
      ("anotherProperty" -> "true")
    )

    val sparkConfig = CassandraOutput.getSparkConfiguration(config)

    sparkConfig.exists(_ == ("spark.cassandra.input.fetch.size_in_rows" -> "2000")) should be(false)
    sparkConfig.exists(_ == ("spark.cassandra.input.split.size_in_mb" -> "64")) should be(false)
    sparkConfig.exists(_ == ("anotherProperty" -> "true")) should be(false)
  }
}
