
package com.kong.eos.serving.api.actor

import java.util.regex.Pattern

import akka.actor.Actor
import com.kong.eos.serving.api.actor.DriverActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.FileActorUtils
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.files.KongFilesResponse
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.FileActorUtils
import com.kong.eos.serving.core.models.files.KongFilesResponse
import spray.http.BodyPart
import spray.httpx.Json4sJacksonSupport

import scala.util.{Failure, Try}

class DriverActor extends Actor with Json4sJacksonSupport with FileActorUtils with KongCloudSerializer {

  //The dir where the jars will be saved
  val targetDir = Try(KongCloudConfig.getDetailConfig.get.getString(AppConstant.DriverPackageLocation))
    .getOrElse(AppConstant.DefaultDriverPackageLocation)
  override val apiPath = HttpConstant.DriverPath
  override val patternFileName = Option(Pattern.compile(""".*\.jar""").asPredicate())

  override def receive: Receive = {
    case UploadDrivers(files) => if (files.isEmpty) errorResponse() else uploadDrivers(files)
    case ListDrivers => browseDrivers()
    case DeleteDrivers => deleteDrivers()
    case DeleteDriver(fileName) => deleteDriver(fileName)
    case _ => log.info("Unrecognized message in Driver Actor")
  }

  def errorResponse(): Unit =
    sender ! KongFilesResponse(Failure(new IllegalArgumentException(s"At least one file is expected")))

  def deleteDrivers(): Unit = sender ! DriverResponse(deleteFiles())

  def deleteDriver(fileName: String): Unit = sender ! DriverResponse(deleteFile(fileName))

  def browseDrivers(): Unit = sender ! KongFilesResponse(browseDirectory())

  def uploadDrivers(files: Seq[BodyPart]): Unit = sender ! KongFilesResponse(uploadFiles(files))
}

object DriverActor {

  case class UploadDrivers(files: Seq[BodyPart])

  case class DriverResponse(status: Try[_])

  case object ListDrivers

  case object DeleteDrivers

  case class DeleteDriver(fileName: String)

}
