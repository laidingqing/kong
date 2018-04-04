
package com.kong.eos.plugin.input.websocket

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.input.Input
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.kong.eos.sdk.properties.ValidatingPropertyMap._

class WebSocketInput(properties: Map[String, JSerializable]) extends Input(properties) {

  def initStream(ssc: StreamingContext, sparkStorageLevel: String): DStream[Row] = {
    ssc.receiverStream(new WebSocketReceiver(properties.getString("url"), storageLevel(sparkStorageLevel)))
      .map(data => Row(data))
  }
}
