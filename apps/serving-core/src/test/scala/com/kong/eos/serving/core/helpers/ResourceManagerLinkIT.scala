
package com.kong.eos.serving.core.helpers

import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

import com.kong.eos.serving.core.config.KongCloudConfig
import com.typesafe.config.ConfigFactory
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResourceManagerLinkIT extends FlatSpec with
  ShouldMatchers with Matchers with BeforeAndAfter {

  var serverSocket: ServerSocketChannel = _
  val sparkUIPort = 4040
  val mesosPort = 5050
  val localhost = "127.0.0.1"

  after {
    serverSocket.close()
  }

  it should "return local Spark UI link" in {
    serverSocket = ServerSocketChannel.open()
    val localhostName = java.net.InetAddress.getLocalHost().getHostName()
    serverSocket.socket.bind(new InetSocketAddress(localhostName, sparkUIPort))
    val config = ConfigFactory.parseString(
      """
        |sparta{
        |  config {
        |    executionMode = local
        |  }
        |}
      """.stripMargin)
    KongCloudConfig.initMainConfig(Option(config))
    ResourceManagerLinkHelper.getLink("local") should be(Some(s"http://${localhostName}:${sparkUIPort}"))
  }

  it should "return Mesos UI link" in {
    serverSocket = ServerSocketChannel.open()
    serverSocket.socket.bind(new InetSocketAddress(localhost,mesosPort))
    val config = ConfigFactory.parseString(
      """
        |sparta{
        |  config {
        |    executionMode = mesos
        |  }
        |
        |  mesos {
        |    master = "mesos://127.0.0.1:7077"
        |  }
        |}
      """.stripMargin)
    KongCloudConfig.initMainConfig(Option(config))
    ResourceManagerLinkHelper.getLink("mesos") should be(Some(s"http://$localhost:$mesosPort"))
  }

}
