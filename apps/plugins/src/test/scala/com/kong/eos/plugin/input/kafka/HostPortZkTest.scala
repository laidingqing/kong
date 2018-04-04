
package com.kong.eos.plugin.input.kafka

import java.io.Serializable

import com.kong.eos.sdk.properties.JsoneyString
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}



@RunWith(classOf[JUnitRunner])
class HostPortZkTest extends WordSpec with Matchers {

  class KafkaTestInput(val properties: Map[String, Serializable]) extends KafkaBase
  
  "getHostPortZk" should {

    "return a chain (zookeper:conection , host:port)" in {
      val conn = """[{"host": "localhost", "port": "2181"}]"""
      val props = Map("zookeeper.connect" -> JsoneyString(conn), "zookeeper.path" -> "")
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181"))
    }

    "return a chain (zookeper:conection , host:port, zookeeper.path:path)" in {
      val conn = """[{"host": "localhost", "port": "2181"}]"""
      val props = Map("zookeeper.connect" -> JsoneyString(conn), "zookeeper.path" -> "/test")
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181/test"))
    }

    "return a chain (zookeper:conection , host:port,host:port,host:port)" in {
      val conn =
        """[{"host": "localhost", "port": "2181"},{"host": "localhost", "port": "2181"},
          |{"host": "localhost", "port": "2181"}]""".stripMargin
      val props = Map("zookeeper.connect" -> JsoneyString(conn))
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181,localhost:2181,localhost:2181"))
    }

    "return a chain (zookeper:conection , host:port,host:port,host:port, zookeeper.path:path)" in {
      val conn =
        """[{"host": "localhost", "port": "2181"},{"host": "localhost", "port": "2181"},
          |{"host": "localhost", "port": "2181"}]""".stripMargin
      val props = Map("zookeeper.connect" -> JsoneyString(conn), "zookeeper.path" -> "/test")
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181,localhost:2181,localhost:2181/test"))
    }

    "return a chain with default port (zookeper:conection , host: defaultport)" in {

      val props = Map("foo" -> "var")
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181"))
    }

    "return a chain with default port (zookeper:conection , host: defaultport, zookeeper.path:path)" in {
      val props = Map("zookeeper.path" -> "/test")
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181/test"))
    }

    "return a chain with default host and default porty (zookeeper.connect: ," +
      "defaultHost: defaultport," +
      "zookeeper.path:path)" in {
      val props = Map("foo" -> "var")
      val input = new KafkaTestInput(props)

      input.getHostPortZk("zookeeper.connect", "localhost", "2181") should
        be(Map("zookeeper.connect" -> "localhost:2181"))
    }
  }
}

