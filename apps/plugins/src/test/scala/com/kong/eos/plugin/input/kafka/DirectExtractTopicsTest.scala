
package com.kong.eos.plugin.input.kafka

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.properties.JsoneyString
import org.scalatest.{Matchers, WordSpec}


class DirectExtractTopicsTest extends WordSpec with Matchers {

  class KafkaTestInput(val properties: Map[String, JSerializable]) extends KafkaBase
  
  "Topics match" should {

    "return a tuples (topic,partition)" in {
      val topics =
        """[
          |{
          |   "topic":"test"
          |}
          |]
          |""".stripMargin

      val properties = Map("topics" -> JsoneyString(topics))
      val input = new KafkaTestInput(properties)
      input.extractTopics should be(Set("test"))
    }

    "return a sequence of tuples (topic,partition)" in {
      val topics =
        """[
          |{"topic":"test"},{"topic":"test2"},{"topic":"test3"}
          |]
          |""".stripMargin
      val properties = Map("topics" -> JsoneyString(topics))
      val input = new KafkaTestInput(properties)
      input.extractTopics should be(Set("test","test2","test3"))
    }
  }
}
