
package com.kong.eos.plugin.input.flume

import java.io.Serializable
import java.net.InetSocketAddress

import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.properties.ValidatingPropertyMap._

import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils

class FlumeInput(properties: Map[String, Serializable]) extends Input(properties) {

  val DEFAULT_FLUME_PORT = 11999
  val DEFAULT_ENABLE_DECOMPRESSION = false
  val DEFAULT_MAXBATCHSIZE = 1000
  val DEFAULT_PARALLELISM = 5

  def initStream(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {

    if (properties.getString("type").equalsIgnoreCase("pull")) {
      FlumeUtils.createPollingStream(
        ssc,
        getAddresses,
        storageLevel(sparkStorageLevel),
        maxBatchSize,
        parallelism
      ).map(data => Row(data.event.getBody.array))
    } else {
      // push
      FlumeUtils.createStream(
        ssc, properties.getString("hostname"),
        properties.getString("port").toInt,
        storageLevel(sparkStorageLevel),
        enableDecompression
      ).map(data => Row(data.event.getBody.array))
    }
  }

  private def getAddresses: Seq[InetSocketAddress] =
    properties.getMapFromJsoneyString("addresses")
      .map(values => (values.get("host"), values.get("port")))
      .map {
        case (Some(address), None) =>
          new InetSocketAddress(address, DEFAULT_FLUME_PORT)
        case (Some(address), Some(port)) =>
          new InetSocketAddress(address, port.toInt)
        case _ =>
          throw new IllegalStateException(s"Invalid configuration value for addresses : ${properties.get("addresses")}")
      }

  private def enableDecompression: Boolean =
    properties.hasKey("enableDecompression") match {
      case true => properties.getBoolean("enableDecompression")
      case false => DEFAULT_ENABLE_DECOMPRESSION
    }

  private def parallelism: Int = {
    properties.hasKey("parallelism") match {
      case true => properties.getString("parallelism").toInt
      case false => DEFAULT_PARALLELISM
    }
  }

  private def maxBatchSize: Int =
    properties.hasKey("maxBatchSize") match {
      case true => properties.getString("maxBatchSize").toInt
      case false => DEFAULT_MAXBATCHSIZE
    }
}

