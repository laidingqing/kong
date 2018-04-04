
package com.kong.eos.plugin.input.rabbitmq

import org.apache.spark.streaming.rabbitmq.models.ExchangeAndRouting
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}

@RunWith(classOf[JUnitRunner])
class RabbitMQDistributedInputTest extends WordSpec with Matchers {

  import RabbitMQDistributedInput._

  "RabbitMQDistributedInput " should {

    "Generate correct RabbitMQDistributedKey for empty input" in {
      val input = new RabbitMQDistributedInput(Map.empty[String, String])
      val result = input.getKey(Map.empty[String, String], Map.empty[String, String])
      result.connectionParams should contain(HostPropertyKey, HostDefaultValue)
      result.connectionParams should have size 1
      result.queue should ===(QueueDefaultValue)
      result.exchangeAndRouting should ===(ExchangeAndRouting(None, None, None))
    }

    "Generate correct RabbitMQDistributedKey for blank input" in {
      val input = new RabbitMQDistributedInput(Map.empty[String, String])

      val params = Map(
        ExchangeNamePropertyKey -> "",
        ExchangeTypePropertyKey -> "   ",
        RoutingKeysPropertyKey -> ""
      )

      val result = input.getKey(params, Map.empty[String, String])
      result.connectionParams should contain(HostPropertyKey, HostDefaultValue)
      result.connectionParams should have size 1
      result.queue should ===(QueueDefaultValue)
      result.exchangeAndRouting should ===(ExchangeAndRouting(None, None, None))
    }

    "Generate correct RabbitMQDistributedKey for real input" in {
      val input = new RabbitMQDistributedInput(Map.empty[String, String])

      val params = Map(
        ExchangeNamePropertyKey -> " exchange",
        ExchangeTypePropertyKey -> "  type ",
        RoutingKeysPropertyKey -> "routing"
      )

      val result = input.getKey(params, Map.empty[String, String])
      result.connectionParams should contain(HostPropertyKey, HostDefaultValue)
      result.connectionParams should have size 1
      result.queue should ===(QueueDefaultValue)
      result.exchangeAndRouting should ===(ExchangeAndRouting(Some("exchange"), Some("type"), Some("routing")))
    }

    "Generate correct RabbitMQDistributedKey for missing properties " in {
      val input = new RabbitMQDistributedInput(Map.empty[String, String])

      val params = Map(
        RoutingKeysPropertyKey -> "routing"
      )

      val result = input.getKey(params, Map.empty[String, String])
      result.connectionParams should contain(HostPropertyKey, HostDefaultValue)
      result.connectionParams should have size 1
      result.queue should ===(QueueDefaultValue)
      result.exchangeAndRouting should ===(ExchangeAndRouting(None, None, Some("routing")))
    }

    "Generate correct queue for missing properties " in {
      val input = new RabbitMQDistributedInput(Map.empty[String, String])

      val params = Map(
        RoutingKeysPropertyKey -> "routing"
      )

      val result = input.getKey(params, Map.empty[String, String])
      result.connectionParams should contain(HostPropertyKey, HostDefaultValue)
      result.connectionParams should have size 1
      result.queue should ===(QueueDefaultValue)
      result.exchangeAndRouting should ===(ExchangeAndRouting(None, None, Some("routing")))
    }

    "Get keys for all distributedProperties " in {
      val distributedProperties =
        """
          |[{
          |   "distributedExchangeName": "distributedExchangeName",
          |   "distributedExchangeType": "distributedExchangeType",
          |   "distributedRoutingKeys": "distributedRoutingKeys",
          |   "hosts": "  host      "
          |  },
          |  {
          |   "distributedExchangeName": "distributedExchangeName1",
          |   "distributedExchangeType": "distributedExchangeType1",
          |   "distributedRoutingKeys": "distributedRoutingKeys1",
          |   "distributedQueue": "  other"
          |  }
          |]
        """.stripMargin

      val props = Map(DistributedPropertyKey -> distributedProperties)
      val input = new RabbitMQDistributedInput(props)
      val result = input.getKeys(props)
      result should have size 2
      val first = result.head
      first.exchangeAndRouting should
        ===(ExchangeAndRouting(
          Some("distributedExchangeName"),
          Some("distributedExchangeType"),
          Some("distributedRoutingKeys")
        ))
      first.queue should ===(QueueDefaultValue)
      first.connectionParams should have size 2
      first.connectionParams should contain(HostPropertyKey, "host")
      val second = result(1)
      second.exchangeAndRouting should
        ===(ExchangeAndRouting(
          Some("distributedExchangeName1"),
          Some("distributedExchangeType1"),
          Some("distributedRoutingKeys1")
        ))
      second.queue should ===("other")
      second.connectionParams should have size 2
      second.connectionParams should contain(HostPropertyKey, HostDefaultValue)
    }
    "Get 0 keys for empty string distributedProperties " in {
      val props = Map(DistributedPropertyKey -> "")
      val input = new RabbitMQDistributedInput(props)
      val result = input.getKeys(props)
      result should have size 0
    }
    "Get 0 keys for empty map" in {
      val props = Map.empty[String, String]
      val input = new RabbitMQDistributedInput(props)
      val result = input.getKeys(props)
      result should have size 0
    }
  }
}
