
package com.kong.eos.plugin.helper

import java.io.{BufferedReader, InputStreamReader}
import akka.event.slf4j.SLF4JLogging
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpPost, HttpUriRequest}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import scala.util.parsing.json.JSON


object VaultHelper extends SLF4JLogging {

  lazy val client: HttpClient = HttpClientBuilder.create().build()
  lazy val jsonTemplate: String = "{ \"token\" : \"_replace_\" }"

  def getTemporalToken(vaultHost: String, token: String): String = {
    val requestUrl = s"$vaultHost/v1/sys/wrapping/wrap"

    log.debug(s"Requesting temporal token: $requestUrl")

    val post = new HttpPost(requestUrl)

    post.addHeader("X-Vault-Token", token)
    post.addHeader("X-Vault-Wrap-TTL", "2000s")
    post.setEntity(new StringEntity(jsonTemplate.replace("_replace_", token)))

    getContentFromResponse(post, "wrap_info")("token").asInstanceOf[String]
  }

  private def getContentFromResponse(uriRequest: HttpUriRequest,
                                     parentField: String): Map[String, Any] = {
    val response = client.execute(uriRequest)
    val rd = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
    val json = JSON.parseFull(
      Stream.continually(rd.readLine()).takeWhile(_ != null).mkString).get.asInstanceOf[Map[String, Any]]

    log.debug(s"getFrom Vault ${json.mkString("\n")}")
    if (response.getStatusLine.getStatusCode != 200) {
      val errors = json("errors").asInstanceOf[List[String]].mkString("\n")
      throw new RuntimeException(errors)
    } else json(parentField).asInstanceOf[Map[String, Any]]
  }
}
