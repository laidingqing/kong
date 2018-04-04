
package com.kong.eos.plugin.input.rabbitmq

import java.util.UUID

import akka.pattern.{ask, gracefulStop}
import akka.testkit.TestProbe
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, ChannelOwner, ConnectionOwner, Consumer}
import com.rabbitmq.client.ConnectionFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.github.sstone.amqp.Amqp._
import scala.concurrent.Await
import com.kong.eos.plugin.input.rabbitmq.RabbitMQDistributedInput.DistributedPropertyKey

@RunWith(classOf[JUnitRunner])
class RabbitMQDistributedInputIT extends RabbitIntegrationSpec {

  val queueName = s"$configQueueName-${this.getClass.getName}-${UUID.randomUUID().toString}"
  val exchangeName = s"$configExchangeName-${this.getClass.getName}-${UUID.randomUUID().toString}"

  val queue = QueueParameters(
    name = queueName,
    passive = false,
    exclusive = false,
    durable = true,
    autodelete = false
  )
  val exchange = ExchangeParameters(
    name = exchangeName,
    passive = false,
    exchangeType = exchangeType,
    durable = true,
    autodelete = false
  )

  override def initRabbitMQ(): Unit = {
    val connFactory = new ConnectionFactory()
    connFactory.setUri(RabbitConnectionURI)
    val conn = system.actorOf(ConnectionOwner.props(connFactory, RabbitTimeOut))
    val probe = TestProbe()
    Amqp.waitForConnection(system, conn).await()
    val consumer = ConnectionOwner.createChildActor(
      conn,
      Consumer.props(listener = Some(probe.ref)),
      timeout = RabbitTimeOut,
      name = Some("RabbitMQ.consumer")
    )
    val producer = ConnectionOwner.createChildActor(
      conn,
      ChannelOwner.props(),
      timeout = RabbitTimeOut,
      name = Some("RabbitMQ.producer")
    )
    Amqp.waitForConnection(system, conn, producer, consumer).await()

    val deleteQueueResult = consumer ? DeleteQueue(queueName)
    Await.result(deleteQueueResult, RabbitTimeOut)
    val deleteExchangeResult = consumer ? DeleteExchange(exchangeName)
    Await.result(deleteExchangeResult, RabbitTimeOut)
    val bindingResult = consumer ? AddBinding(Binding(exchange, queue, routingKey))
    Await.result(bindingResult, RabbitTimeOut)

    //Send some messages to the queue
    val results = for (register <- 1 to totalRegisters)
      yield producer ? Publish(
        exchange = exchange.name,
        key = "",
        body = register.toString.getBytes
      )
    results.map(result => Await.result(result, RabbitTimeOut))


    /**
      * Close Producer actors and connections
      */
    conn ! Close()
    Await.result(gracefulStop(conn, RabbitTimeOut), RabbitTimeOut * 2)
    Await.result(gracefulStop(consumer, RabbitTimeOut), RabbitTimeOut * 2)
    Await.result(gracefulStop(producer, RabbitTimeOut), RabbitTimeOut * 2)

  }

  override def closeRabbitMQ(): Unit = {
    val connFactory = new ConnectionFactory()
    connFactory.setUri(RabbitConnectionURI)
    val conn = system.actorOf(ConnectionOwner.props(connFactory, RabbitTimeOut))
    Amqp.waitForConnection(system, conn).await()
    val consumer = ConnectionOwner.createChildActor(
      conn,
      Consumer.props(listener = None),
      timeout = RabbitTimeOut,
      name = Some("RabbitMQ.consumer")
    )
    val deleteQueueResult = consumer ? DeleteQueue(queueName)
    Await.result(deleteQueueResult, RabbitTimeOut)

    /**
      * Close Consumer actors and connections
      */
    conn ! Close()
    Await.result(gracefulStop(conn, RabbitTimeOut), RabbitTimeOut * 2)
    Await.result(gracefulStop(consumer, RabbitTimeOut), RabbitTimeOut * 2)
  }


  "RabbitMQDistributedInput " should {

    "Read all the records" in {
      val distributedProperties =s"""
                                    |[{
                                    |   "distributedExchangeName": "$exchangeName",
                                    |   "distributedExchangeType": "$exchangeType",
                                    |   "hosts": "  $hosts      ",
                                    |   "distributedQueue": "$queueName"
                                    |  }
                                    |]
        """.stripMargin

      val props = Map(DistributedPropertyKey -> distributedProperties)

      val input = new RabbitMQDistributedInput(props)
      val distributedStream = input.initStream(ssc.get, DefaultStorageLevel)
      val totalEvents = sc.get.longAccumulator("Number of events received")

      // Fires each time the configured window has passed.
      distributedStream.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val count = rdd.count()
          // Do something with this message
          log.info(s"EVENTS COUNT : $count")
          totalEvents.add(count)
        } else log.info("RDD is empty")
        log.info(s"TOTAL EVENTS : $totalEvents")
      })

      ssc.get.start() // Start the computation
      ssc.get.awaitTerminationOrTimeout(SparkTimeOut) // Wait for the computation to terminate

      totalEvents.value should ===(totalRegisters.toLong)
    }
  }
}