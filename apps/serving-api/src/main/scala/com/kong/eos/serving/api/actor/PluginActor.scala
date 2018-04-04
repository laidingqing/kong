
package com.kong.eos.serving.api.actor

import java.util.regex.Pattern

import akka.actor.Actor
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.FileActorUtils
import com.kong.eos.serving.api.actor.PluginActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.FileActorUtils
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.files.KongFilesResponse
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.FileActorUtils
import spray.http.BodyPart
import spray.httpx.Json4sJacksonSupport

import scala.util.{Failure, Try}

class PluginActor extends Actor with Json4sJacksonSupport with FileActorUtils with KongCloudSerializer {

  //The dir where the jars will be saved
  val targetDir = Try(KongCloudConfig.getDetailConfig.get.getString(AppConstant.PluginsPackageLocation))
    .getOrElse(AppConstant.DefaultPluginsPackageLocation)
  val apiPath = HttpConstant.PluginsPath
  override val patternFileName = Option(Pattern.compile(""".*\.jar""").asPredicate())

  override def receive: Receive = {
    case UploadPlugins(files) => if (files.isEmpty) errorResponse() else uploadPlugins(files)
    case ListPlugins => browsePlugins()
    case DeletePlugins => deletePlugins()
    case DeletePlugin(fileName) => deletePlugin(fileName)
    case _ => log.info("Unrecognized message in Plugin Actor")
  }

  def errorResponse(): Unit =
    sender ! KongFilesResponse(Failure(new IllegalArgumentException(s"At least one file is expected")))

  def deletePlugins(): Unit = sender ! PluginResponse(deleteFiles())

  def deletePlugin(fileName: String): Unit = sender ! PluginResponse(deleteFile(fileName))

  def browsePlugins(): Unit = sender ! KongFilesResponse(browseDirectory())

  def uploadPlugins(files: Seq[BodyPart]): Unit = sender ! KongFilesResponse(uploadFiles(files))
}

object PluginActor {

  case class UploadPlugins(files: Seq[BodyPart])

  case class PluginResponse(status: Try[_])

  case object ListPlugins

  case object DeletePlugins

  case class DeletePlugin(fileName: String)

}
