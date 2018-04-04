
package com.kong.eos.plugin.input.rabbitmq

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class RabbitMQInputTest extends WordSpec with Matchers {

  val DefaultStorageLevel = "MEMORY_AND_DISK_SER_2"
  val customKey = "inputOptions"
  val customPropertyKey = "inputOptionsKey"
  val customPropertyValue = "inputOptionsValue"

  "RabbitMQInput " should {

    "Add storage level to properties" in {
      val input = new RabbitMQInput(Map.empty[String, String])
      val result = input.propsWithStorageLevel(DefaultStorageLevel)
      result should contain("storageLevel", DefaultStorageLevel)
      result should have size 1
    }

    "Add queue and host to properties" in {
      val props = Map(
        "host" -> "host",
        "queue" -> "queue")
      val input = new RabbitMQInput(props)
      val result = input.propsWithStorageLevel(DefaultStorageLevel)
      result should contain("host", "host")
      result should contain("queue", "queue")
      result should contain("storageLevel", DefaultStorageLevel)
      result should have size 3
    }

    "Flat rabbitmqProperties " in {
      val rabbitmqProperties =
        s"""
          |[{
          |   "$customPropertyKey": "host",
          |   "$customPropertyValue": "host1"
          |  },
          |  {
          |   "$customPropertyKey": "queue",
          |   "$customPropertyValue": "queue1"
          |  }
          |]
        """.stripMargin
      val props = Map(customKey -> rabbitmqProperties)
      val input = new RabbitMQInput(props)
      val result = input.propsWithStorageLevel(DefaultStorageLevel)
      result should contain("host", "host1")
      result should contain("queue", "queue1")
      result should contain("storageLevel", DefaultStorageLevel)
      result should have size 4
    }

    "Fail when no inputOptionsValue in customProperties " in {
      val rabbitmqProperties =
        s"""
          |[{
          |   "$customPropertyKey": "host"
          |}]
        """.stripMargin
      val props = Map(customKey -> rabbitmqProperties)
      the[IllegalStateException] thrownBy {
        new RabbitMQInput(props)
      } should have message "The field inputOptionsValue is mandatory"

    }

    "Fail when no inputOptionsKey in customProperties" in {
      val rabbitmqProperties =
        s"""
          |[{
          |   "$customPropertyValue": "host1"
          |}]
        """.stripMargin
      val props = Map(customKey -> rabbitmqProperties)
      the[IllegalStateException] thrownBy {
        new RabbitMQInput(props)
      } should have message "The field inputOptionsKey is mandatory"
    }

    "Should preserve the UI fields " in {
      val rabbitmqProperties =
        s"""
           |[{
           |   "$customPropertyKey": "host",
           |   "$customPropertyValue": "host1"
           |  },
           |  {
           |   "$customPropertyKey": "queue",
           |   "$customPropertyValue": "queue1"
           |  }
           |]
        """.stripMargin

      val props = Map(
        "host" -> "host",
        "queue" -> "queue",
        customKey -> rabbitmqProperties
      )
      val input = new RabbitMQInput(props)
      val result = input.propsWithStorageLevel(DefaultStorageLevel)
      result should contain("host", "host")
      result should contain("queue", "queue")
      result should contain("storageLevel", DefaultStorageLevel)
      result should have size 4
    }
  }
}
