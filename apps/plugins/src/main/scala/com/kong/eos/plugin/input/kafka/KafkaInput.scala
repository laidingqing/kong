
package com.kong.eos.plugin.input.kafka

import java.io.{Serializable => JSerializable}
import java.lang.{Double, Long}
import java.nio.ByteBuffer

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.plugin.helper.VaultHelper
import com.kong.eos.sdk.pipeline.input.Input
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._

import scala.util.Try

class KafkaInput(properties: Map[String, JSerializable]) extends Input(properties) with KafkaBase with SLF4JLogging {

  val KeyDeserializer = "key.deserializer"
  val ValueDeserializer = "value.deserializer"

  //scalastyle:off
  def initStream(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {
    val groupId = getGroupId("group.id")
    val metaDataBrokerList = if (properties.contains("metadata.broker.list"))
      getHostPort("metadata.broker.list", DefaultHost, DefaultBrokerPort)
    else getHostPort("bootstrap.servers", DefaultHost, DefaultBrokerPort)
    val keySerializer = classOf[StringDeserializer]
    val serializerProperty = properties.getString("value.deserializer", "string")
    val valueSerializer = getSerializerByKey(serializerProperty)
    val serializers = Map(KeyDeserializer -> keySerializer, ValueDeserializer -> valueSerializer)
    val topics = extractTopics
    val partitionStrategy = getPartitionStrategy
    val locationStrategy = getLocationStrategy
    val autoOffset = getAutoOffset
    val enableAutoCommit = getAutoCommit
    val kafkaSecurityOptions = securityOptions(ssc.sparkContext.getConf)

    val inputDStream = serializerProperty match {
      case "long" =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, Long](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, Long](ssc, locationStrategy, consumerStrategy)
      case "int" =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, Int](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, Int](ssc, locationStrategy, consumerStrategy)
      case "double" =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, Double](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, Double](ssc, locationStrategy, consumerStrategy)
      case "bytebuffer" =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, ByteBuffer](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, ByteBuffer](ssc, locationStrategy, consumerStrategy)
      case "arraybyte" =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, Array[Byte]](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, Array[Byte]](ssc, locationStrategy, consumerStrategy)
      case "bytes" =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, Bytes](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, Bytes](ssc, locationStrategy, consumerStrategy)
      case _ =>
        val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics, enableAutoCommit ++
          autoOffset ++ serializers ++ metaDataBrokerList ++ groupId ++ partitionStrategy ++
          kafkaSecurityOptions ++ getCustomProperties)
        KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategy)
    }

    if (!enableAutoCommit.head._2 && getAutoCommitInKafka) {
      inputDStream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        inputDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        log.info(s"Committed Kafka offsets --> ${
          offsetRanges.map(offset =>
            s"\tTopic: ${offset.topic}, Partition: ${offset.partition}, From: ${offset.fromOffset}, until: " +
              s"${offset.untilOffset}"
          ).mkString("\n")
        }")
      }
    }

    inputDStream.map(data => Row(data.value()))
  }

  //scalastyle:on

  /** SERIALIZERS **/

  def getSerializerByKey(serializerKey: String): Class[_ >: StringDeserializer with LongDeserializer
    with IntegerDeserializer with DoubleDeserializer with ByteArrayDeserializer with ByteBufferDeserializer
    with BytesDeserializer <: Deserializer[_ >: String with Long with Integer with Double with
    Array[Byte] with ByteBuffer with Bytes]] =
    serializerKey match {
      case "string" => classOf[StringDeserializer]
      case "long" => classOf[LongDeserializer]
      case "int" => classOf[IntegerDeserializer]
      case "double" => classOf[DoubleDeserializer]
      case "arraybyte" => classOf[ByteArrayDeserializer]
      case "bytebuffer" => classOf[ByteBufferDeserializer]
      case "bytes" => classOf[BytesDeserializer]
      case _ => classOf[StringDeserializer]
    }

  /** OFFSETS MANAGEMENT **/

  def getAutoOffset: Map[String, String] = {
    val autoOffsetResetKey = "auto.offset.reset"
    val autoOffsetResetValue = properties.getString(autoOffsetResetKey, "latest")

    Map(autoOffsetResetKey -> autoOffsetResetValue)
  }

  def getAutoCommit: Map[String, java.lang.Boolean] = {
    val autoCommitKey = "enable.auto.commit"
    val autoCommitValue = Try(properties.getBoolean(autoCommitKey)).getOrElse(false)

    Map(autoCommitKey -> autoCommitValue)
  }

  def getAutoCommitInKafka: Boolean =
    Try(properties.getBoolean("storeOffsetInKafka")).getOrElse(true)

  /** LOCATION STRATEGY **/

  def getLocationStrategy: LocationStrategy =
    properties.getString("locationStrategy", None) match {
      case Some(strategy) => strategy match {
        case "preferbrokers" => LocationStrategies.PreferBrokers
        case "preferconsistent" => LocationStrategies.PreferConsistent
        case _ => LocationStrategies.PreferConsistent
      }
      case None => LocationStrategies.PreferConsistent
    }

  /** PARTITION ASSIGNMENT STRATEGY **/

  def getPartitionStrategy: Map[String, String] = {
    val partitionStrategyKey = "partition.assignment.strategy"
    val strategy = properties.getString("partition.assignment.strategy", None) match {
      case Some("range") => classOf[RangeAssignor].getCanonicalName
      case Some("roundrobin") => classOf[RoundRobinAssignor].getCanonicalName
      case None => classOf[RangeAssignor].getCanonicalName
    }

    Map(partitionStrategyKey -> strategy)
  }

  def securityOptions(sparkConf: SparkConf): Map[String, AnyRef] = {
    if (sparkConf.contains("spark.secret.kafka.security.protocol")) {
      sparkConf.getAll.flatMap { case (key, value) =>
        if (key.startsWith("spark.secret.kafka.")) {
          Option((key.split("spark.secret.kafka.").tail.head.toLowerCase, value))
        } else None
      }.toMap
    } else Map.empty[String, AnyRef]
  }
}

object KafkaInput {

  def getSparkSubmitConfiguration(configuration: Map[String, JSerializable]): Seq[(String, String)] = {
    val vaultHost = scala.util.Properties.envOrNone("VAULT_HOST")
    val vaultToken = scala.util.Properties.envOrNone("VAULT_TOKEN")
    val vaultCertPath = configuration.getString("vaultCertPath", None)
    val vaultCertPassPath = configuration.getString("vaultCertPassPath", None)
    val vaultKeyPassPath = configuration.getString("vaultKeyPassPath", None)

    (vaultHost, vaultToken, vaultCertPath, vaultCertPassPath, vaultKeyPassPath) match {
      case (Some(host), Some(token), Some(certPath), Some(certPassPath), Some(keyPassPath)) =>
        Seq(
          ("spark.secret.kafka.security.protocol", "SSL"),
          ("spark.mesos.executor.docker.volumes",
            "/etc/pki/ca-trust/extracted/java/cacerts/:/etc/ssl/certs/java/cacerts:ro"),
          ("spark.mesos.driverEnv.KAFKA_VAULT_CERT_PATH", certPath),
          ("spark.mesos.driverEnv.KAFKA_VAULT_CERT_PASS_PATH", certPassPath),
          ("spark.mesos.driverEnv.KAFKA_VAULT_KEY_PASS_PATH", keyPassPath),
          ("spark.executorEnv.VAULT_HOST", host),
          ("spark.executorEnv.KAFKA_VAULT_CERT_PATH", certPath),
          ("spark.executorEnv.KAFKA_VAULT_CERT_PASS_PATH", certPassPath),
          ("spark.executorEnv.KAFKA_VAULT_KEY_PASS_PATH", keyPassPath),
          ("spark.secret.vault.host", host),
          ("spark.secret.vault.tempToken", VaultHelper.getTemporalToken(host, token))
        )
      case _ => Seq.empty[(String, String)]
    }
  }
}
