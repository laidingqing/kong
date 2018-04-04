
package com.kong.eos.serving.core.constants

/**
 * Akka constants with Akka's actor paths.
 *
 */
object AkkaConstant {

  val FragmentActorName = "fragmentActor"
  val PolicyActorName = "policyActor"
  val ExecutionActorName = "executionActor"
  val ClusterLauncherActorName = "clusterLauncherActor"
  val LauncherActorName = "launcherActor"
  val PluginActorName = "pluginActor"
  val DriverActorName = "driverActor"
  val ControllerActorName = "controllerActor"
  val StatusActorName = "statusActor"
  val MarathonAppActorName = "marathonAppActor"
  val UpDownMarathonActor = "upDownMarathonActor"
  val ConfigActorName = "configurationActor"
  val MetadataActorName = "metadataActor"
  val VisualizationActorName = "visualizationActor"
  val DefaultTimeout = 15

  def cleanActorName(initialName: String): String = initialName.replace(" ", "_")
}
