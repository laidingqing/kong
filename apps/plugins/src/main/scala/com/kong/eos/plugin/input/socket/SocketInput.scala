
package com.kong.eos.plugin.input.socket

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.input.Input
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class SocketInput(properties: Map[String, JSerializable]) extends Input(properties) {

  private val hostname : String = properties.getString("hostname")
  private val port : Int = properties.getInt("port")

  def initStream(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {
    ssc.socketTextStream(
      hostname,
      port,
      storageLevel(sparkStorageLevel))
      .map(data => Row(data))
  }
}
