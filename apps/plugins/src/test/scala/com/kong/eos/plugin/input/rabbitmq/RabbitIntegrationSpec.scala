
package com.kong.eos.plugin.input.rabbitmq

import akka.actor.ActorSystem
import akka.event.slf4j.SLF4JLogging
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.{Minute, Span}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpec}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

abstract class RabbitIntegrationSpec extends WordSpec with Matchers with SLF4JLogging with TimeLimitedTests
  with BeforeAndAfter with BeforeAndAfterAll {
  private lazy val config = ConfigFactory.load()


  implicit val system = ActorSystem("ActorRabbitMQSystem")
  implicit val timeout = Timeout(10 seconds)
  val timeLimit = Span(1, Minute)
  /**
    * Spark Properties
    */
  val DefaultStorageLevel = "MEMORY_AND_DISK_SER_2"
  val DefaultSparkTimeOut = 3000L
  val SparkTimeOut = Try(config.getLong("spark.timeout")).getOrElse(DefaultSparkTimeOut)
  val conf = new SparkConf()
    .setAppName("RabbitIntegrationSpec")
    .setIfMissing("spark.master", "local[*]")
  //Total messages to send and receive
  val totalRegisters = 10000
  /**
    * RabbitMQ Properties
    */
  val RabbitTimeOut = 3 second
  val configQueueName = Try(config.getString("rabbitmq.queueName")).getOrElse("rabbitmq-queue")
  val configExchangeName = Try(config.getString("rabbitmq.exchangeName")).getOrElse("rabbitmq-exchange")
  val exchangeType = Try(config.getString("rabbitmq.exchangeType")).getOrElse("topic")
  val routingKey = Try(config.getString("rabbitmq.routingKey")).getOrElse("")
  val vHost = Try(config.getString("rabbitmq.vHost")).getOrElse("/")
  val hosts = Try(config.getString("rabbitmq.hosts")).getOrElse("127.0.0.1")
  val userName = Try(config.getString("rabbitmq.userName")).getOrElse("guest")
  val password = Try(config.getString("rabbitmq.password")).getOrElse("guest")
  val RabbitConnectionURI = s"amqp://$userName:$password@$hosts/%2F"
  var sc: Option[SparkContext] = None
  var ssc: Option[StreamingContext] = None

  def initSpark(): Unit = {
    sc = Some(new SparkContext(conf))
    ssc = Some(new StreamingContext(sc.get, Seconds(1)))
  }

  def stopSpark(): Unit = {
    ssc.foreach(_.stop())
    sc.foreach(_.stop())

    System.gc()
  }

  def initRabbitMQ(): Unit

  def closeRabbitMQ(): Unit

  before {
    log.info("Init spark")
    initSpark()
    log.info("Sending messages to queue..")
    initRabbitMQ()
    log.info("Messages in queue.")
  }

  after {
    log.info("Stop spark")
    stopSpark()
    log.info("Clean rabbitmq")
    closeRabbitMQ()
  }
}
