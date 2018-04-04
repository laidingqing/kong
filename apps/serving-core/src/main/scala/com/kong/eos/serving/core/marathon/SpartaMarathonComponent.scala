
package com.kong.eos.serving.core.marathon

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods._
import akka.stream.ActorMaterializer
import com.stratio.tikitakka.common.exceptions.ConfigurationException
import com.stratio.tikitakka.common.util.ConfigComponent
import com.stratio.tikitakka.updown.marathon.MarathonComponent
import com.kong.eos.serving.core.marathon.SpartaMarathonComponent._

trait SpartaMarathonComponent extends MarathonComponent {

  override lazy val uri = ConfigComponent.getString(SpartaMarathonComponent.uriField).getOrElse {
    throw ConfigurationException("The marathon uri has not been set")
  }

  override lazy val apiVersion = ConfigComponent.getString(versionField, defaultApiVersion)
}

object SpartaMarathonComponent {

  // Property field constants
  val uriField = "sparta.marathon.tikitakka.marathon.uri"
  val versionField = "sparta.marathon.tikitakka.marathon.api.version"

  // Default property constants
  val defaultApiVersion = "v2"

  val upComponentMethod = POST
  val downComponentMethod = DELETE

  def apply(implicit _system: ActorSystem, _materializer: ActorMaterializer): SpartaMarathonComponent =
    new SpartaMarathonComponent {
      implicit val actorMaterializer: ActorMaterializer = _materializer
      implicit val system: ActorSystem = _system
    }
}
