
package com.kong.eos.serving.api.actor

import java.util.regex.Pattern

import akka.actor.Actor
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.{BackupRestoreUtils, FileActorUtils}
import com.github.nscala_time.time.Imports.{DateTime, DateTimeFormat}
import com.kong.eos.serving.api.actor.MetadataActor.ExecuteBackup
import com.kong.eos.serving.api.actor.MetadataActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.{BackupRestoreUtils, FileActorUtils}
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant._
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.helpers.InfoHelper
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.files.{BackupRequest, KongFilesResponse}
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.utils.{BackupRestoreUtils, FileActorUtils}
import spray.http.BodyPart
import spray.httpx.Json4sJacksonSupport

import scala.util.{Failure, Success, Try}

class MetadataActor extends Actor with Json4sJacksonSupport with BackupRestoreUtils with KongCloudSerializer
  with FileActorUtils {

  //The dir where the backups will be saved
  val targetDir = Try(KongCloudConfig.getDetailConfig.get.getString(BackupsLocation)).getOrElse(DefaultBackupsLocation)
  override val apiPath = HttpConstant.MetadataPath
  override val patternFileName = Option(Pattern.compile(""".*\.json""").asPredicate())

  //The dir where the jars will be saved
  val zkConfig = Try(KongCloudConfig.getZookeeperConfig.get)
    .getOrElse(throw new ServingCoreException("Zookeeper configuration is mandatory"))
  override val uri = Try(zkConfig.getString("connectionString")).getOrElse(DefaultZKConnection)
  override val connectionTimeout = Try(zkConfig.getInt("connectionTimeout")).getOrElse(DefaultZKConnectionTimeout)
  override val sessionTimeout = Try(zkConfig.getInt("sessionTimeout")).getOrElse(DefaultZKSessionTimeout)

  override def receive: Receive = {
    case UploadBackups(files) => if (files.isEmpty) errorResponse() else uploadBackups(files)
    case ListBackups => browseBackups()
    case BuildBackup => buildBackup()
    case DeleteBackups => deleteBackups()
    case CleanMetadata => cleanMetadata()
    case DeleteBackup(fileName) => deleteBackup(fileName)
    case ExecuteBackup(backupRequest) => executeBackup(backupRequest)
    case _ => log.info("Unrecognized message in Backup/Restore Actor")
  }

  def executeBackup(backupRequest: BackupRequest): Unit =
    sender ! BackupResponse(Try{
      importer("/", s"$targetDir/${backupRequest.fileName}", backupRequest.deleteAllBefore)
    })

  def errorResponse(): Unit =
    sender ! KongFilesResponse(Failure(new IllegalArgumentException(s"At least one file is expected")))

  def deleteBackups(): Unit = sender ! BackupResponse(deleteFiles())

  def cleanMetadata(): Unit = sender ! BackupResponse(Try(cleanZk(BaseZKPath)))

  def buildBackup(): Unit = {
    val format = DateTimeFormat.forPattern("yyyy-MM-dd-hh:mm:ss")
    val appInfo = InfoHelper.getAppInfo
    Try {
      dump(BaseZKPath, s"$targetDir/backup-${format.print(DateTime.now)}-${appInfo.pomVersion}.json")
    } match {
      case Success(_) =>
        sender ! KongFilesResponse(browseDirectory())
      case Failure(e) =>
        sender ! BackupResponse(Try(throw e))
    }
  }

  def deleteBackup(fileName: String): Unit = sender ! BackupResponse(deleteFile(fileName))

  def browseBackups(): Unit = sender ! KongFilesResponse(browseDirectory())

  def uploadBackups(files: Seq[BodyPart]): Unit = sender ! KongFilesResponse(uploadFiles(files))
}

object MetadataActor {

  case class UploadBackups(files: Seq[BodyPart])

  case class BackupResponse(status: Try[_])

  case class ExecuteBackup(backupRequest: BackupRequest)

  case object ListBackups

  case object BuildBackup

  case object DeleteBackups

  case object CleanMetadata

  case class DeleteBackup(fileName: String)

}

