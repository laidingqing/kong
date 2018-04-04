
package com.kong.eos.serving.api.service.ssl

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import com.kong.eos.serving.api.helpers.KongCloudHelper.log
import com.kong.eos.serving.core.config.KongCloudConfig
import spray.io._

import scala.util.{Failure, Success, Try}

trait SSLSupport {

  implicit def sslContext: SSLContext = {
    val context = SSLContext.getInstance("TLS")
    if(isHttpsEnabled) {
      val keyStoreResource = KongCloudConfig.apiConfig.get.getString("certificate-file")
      val password = KongCloudConfig.apiConfig.get.getString("certificate-password")

      val keyStore = KeyStore.getInstance("jks")
      keyStore.load(new FileInputStream(keyStoreResource), password.toCharArray)
      val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
      keyManagerFactory.init(keyStore, password.toCharArray)
      val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
      trustManagerFactory.init(keyStore)
      context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
    }
    context
  }

  implicit def sslEngineProvider: ServerSSLEngineProvider = {
    ServerSSLEngineProvider { engine =>
      engine.setEnabledCipherSuites(Array(
        "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384", "TLS_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384", "TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"))
      engine.setEnabledProtocols(Array( "TLSv1.2" ))
      engine
    }
  }

  def isHttpsEnabled: Boolean =
    KongCloudConfig.getSprayConfig match {
      case Some(config) =>
        Try(config.getValue("ssl-encryption")) match {
          case Success(value) =>
            "on".equals(value.unwrapped())
          case Failure(e) =>
            log.error("Incorrect value in ssl-encryption option, setting https disabled", e)
            false
        }
      case None =>
        log.warn("Impossible to get spray config, setting https disabled")
        false
    }
}
