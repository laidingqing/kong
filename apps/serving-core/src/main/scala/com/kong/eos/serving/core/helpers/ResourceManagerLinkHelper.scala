
package com.kong.eos.serving.core.helpers

import java.net.Socket

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant

import scala.util._

object ResourceManagerLinkHelper extends SLF4JLogging {

  def getLink(executionMode : String, monitoringLink: Option[String] = None): Option[String] = {
    val (host: String, port: Int) = (monitoringLink, executionMode) match {
      case (None, AppConstant.ConfigMesos) | (None, AppConstant.ConfigMarathon) => mesosLink
      case (None, AppConstant.ConfigLocal) => localLink
      case (Some(uri), _) => userLink(uri)
      case _ => throw new IllegalArgumentException(s"Wrong value in property executionMode: $executionMode")
    }

    checkConnectivity(host, port)
  }

  def checkConnectivity(host: String, port: Int, monitoringLink: Option[String] = None): Option[String] = {
    Try {
      new Socket(host, port)
    } match {
      case Success(socket) =>
        if (socket.isConnected) {
          socket.close()
          monitoringLink.orElse(Option(s"http://$host:$port"))
        } else {
          log.debug(s"Cannot connect to http://$host:$port")
          socket.close()
          monitoringLink
        }
      case Failure(_) =>
        log.debug(s"Cannot connect to http://$host:$port")
        monitoringLink
    }
  }

  private def mesosLink = {
    val mesosDispatcherUrl = KongCloudConfig.getClusterConfig().get.getString(AppConstant.MesosMasterDispatchers)
    val host = mesosDispatcherUrl.replace("mesos://", "").replaceAll(":\\d+", "")
    val port = 5050
    (host, port)
  }

  private def localLink = {
    val localhostName = java.net.InetAddress.getLocalHost.getHostName
    (localhostName, 4040)
  }

  private def userLink(uri: String) = {
    val host = uri.replace("http://", "").replace("https://", "").replaceAll(":\\d+", "")
    val port = uri.split(":").lastOption.getOrElse("4040").toInt
    (host, port)
  }
}
