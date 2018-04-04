
package com.kong.eos.plugin.input.websocket

import akka.event.slf4j.SLF4JLogging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class WebSocketReceiver(url: String, storageLevel: StorageLevel)
  extends Receiver[String](storageLevel) with SLF4JLogging {


  private var webSocket: Option[WebSocket] = None

  def onStart() {
    try {
      log.info("Connecting to WebSocket: " + url)
      val newWebSocket = WebSocket().open(url)
        .onTextMessage({ msg: String => store(msg) })
        .onBinaryMessage({ msg: Array[Byte] => store(new Predef.String(msg)) })
      setWebSocket(Option(newWebSocket))
      log.info("Connected to: WebSocket" + url)
    } catch {
      case e: Exception => restart("Error starting WebSocket stream", e)
    }
  }

  def onStop() {
    setWebSocket()
    log.info("WebSocket receiver stopped")
  }

  private def setWebSocket(newWebSocket: Option[WebSocket] = None) = synchronized {
    if (webSocket.isDefined)
      webSocket.get.shutdown()
    webSocket = newWebSocket
  }

}
