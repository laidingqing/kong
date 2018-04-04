
package com.kong.eos.serving.api.helpers

import akka.actor.{ActorSystem, Props}
import akka.event.slf4j.SLF4JLogging
import akka.io.IO
import com.kong.eos.serving.api.service.ssl.SSLSupport
import com.kong.eos.driver.service.StreamingContextService
import com.kong.eos.serving.api.actor._
import com.kong.eos.serving.core.actor.StatusActor.AddClusterListeners
import com.kong.eos.serving.core.actor.{FragmentActor, RequestActor, StatusActor}
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AkkaConstant._
import com.kong.eos.serving.core.storage.StorageFactoryHolder
import com.kong.eos.serving.api.service.ssl.SSLSupport
import spray.can.Http

/**
 * Helper with common operations used to create a Kong Cloud context used to run the application.
 */
object KongCloudHelper extends SLF4JLogging with SSLSupport {

  /**
   * Initializes Cloud's akka system running an embedded http server with the REST API.
   *
   */
  def initCloudAPI(appName: String): Unit = {
    if (KongCloudConfig.mainConfig.isDefined && KongCloudConfig.apiConfig.isDefined) {

      val mongoClient = StorageFactoryHolder.getInstance()

      log.info("Initializing Kong Cloud Actors System ...")
      implicit val system = ActorSystem(appName, KongCloudConfig.mainConfig)

      val statusActor = system.actorOf(Props(new StatusActor()), StatusActorName)
      val fragmentActor = system.actorOf(Props(new FragmentActor()), FragmentActorName)
      val policyActor = system.actorOf(Props(new PolicyActor(statusActor)), PolicyActorName)
      val executionActor = system.actorOf(Props(new RequestActor()), ExecutionActorName)
      val scService = StreamingContextService(KongCloudConfig.mainConfig)
      val launcherActor = system.actorOf(Props(new LauncherActor(scService)), LauncherActorName)
      val pluginActor = system.actorOf(Props(new PluginActor()), PluginActorName)
      val driverActor = system.actorOf(Props(new DriverActor()), DriverActorName)
      val configActor = system.actorOf(Props(new ConfigActor()), ConfigActorName)
      val metadataActor = system.actorOf(Props(new MetadataActor()), MetadataActorName)
      val actors = Map(
        StatusActorName -> statusActor,
        FragmentActorName -> fragmentActor,
        PolicyActorName -> policyActor,
        LauncherActorName -> launcherActor,
        PluginActorName -> pluginActor,
        DriverActorName -> driverActor,
        ExecutionActorName -> executionActor,
        ConfigActorName -> configActor,
        MetadataActorName -> metadataActor
      )
      val controllerActor = system.actorOf(Props(new ControllerActor(actors)), ControllerActorName)
      val interface = KongCloudConfig.apiConfig.get.getString("host")
      val port = KongCloudConfig.apiConfig.get.getInt("port")

      IO(Http) ! Http.Bind(controllerActor,
        interface,
        port
      )
      log.info(s"Kong Cloud Actors System initiated correctly, port: $port")

      statusActor ! AddClusterListeners
    } else log.info("Kong Cloud Configuration is not defined")
  }

}
