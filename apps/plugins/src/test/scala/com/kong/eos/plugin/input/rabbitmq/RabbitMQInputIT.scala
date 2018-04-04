
package com.kong.eos.plugin.input.rabbitmq

import java.util.UUID

import akka.pattern.{ask, gracefulStop}
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.{Amqp, ChannelOwner, ConnectionOwner, Consumer}
import com.rabbitmq.client.ConnectionFactory
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class RabbitMQInputIT extends RabbitIntegrationSpec {

  val queueName = s"$configQueueName-${this.getClass.getName}-${UUID.randomUUID().toString}"

  def initRabbitMQ(): Unit = {
    val connFactory = new ConnectionFactory()
    connFactory.setUri(RabbitConnectionURI)
    val conn = system.actorOf(ConnectionOwner.props(connFactory, RabbitTimeOut))
    val producer = ConnectionOwner.createChildActor(
      conn,
      ChannelOwner.props(),
      timeout = RabbitTimeOut,
      name = Some("RabbitMQ.producer")
    )

    val queue = QueueParameters(
      name = queueName,
      passive = false,
      exclusive = false,
      durable = true,
      autodelete = false
    )

    Amqp.waitForConnection(system, conn, producer).await()

    val deleteQueueResult = producer ? DeleteQueue(queueName)
    Await.result(deleteQueueResult, RabbitTimeOut)
    val createQueueResult = producer ? DeclareQueue(queue)
    Await.result(createQueueResult, RabbitTimeOut)

    //Send some messages to the queue
    val results = for (register <- 1 to totalRegisters)
      yield producer ? Publish(
        exchange = "",
        key = queueName,
        body = register.toString.getBytes
      )
    results.map(result => Await.result(result, RabbitTimeOut))
    /**
      * Close Producer actor and connections
      */
    conn ! Close()
    Await.result(gracefulStop(conn, RabbitTimeOut), RabbitTimeOut * 2)
    Await.result(gracefulStop(producer, RabbitTimeOut), RabbitTimeOut * 2)
  }

  def closeRabbitMQ(): Unit = {
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
      * Close consumer actor and connections
      */
    conn ! Close()
    Await.result(gracefulStop(conn, RabbitTimeOut), RabbitTimeOut * 2)
    Await.result(gracefulStop(consumer, RabbitTimeOut), RabbitTimeOut * 2)
  }


  "RabbitMQInput " should {

    "Read all the records" in {
      val props = Map(
        "hosts" -> hosts,
        "queueName" -> queueName)

      val input = new RabbitMQInput(props)
      val distributedStream = input.initStream(ssc.get, DefaultStorageLevel)
      val totalEvents = ssc.get.sparkContext.accumulator(0L, "Number of events received")

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
