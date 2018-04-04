
package com.kong.eos.serving.api.service.http

import javax.ws.rs.Path

import akka.pattern.ask
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.MetadataActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.api.oauth2.OauthClient
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.core.models.files.{BackupRequest, KongFilesResponse}
import com.wordnik.swagger.annotations._
import spray.http._
import spray.httpx.unmarshalling.{FormDataUnmarshallers, Unmarshaller}
import spray.routing.Route

import scala.util.{Failure, Success, Try}

@Api(value = HttpConstant.MetadataPath, description = "Operations over Sparta metadata")
trait MetadataHttpService extends BaseHttpService with OauthClient {

  implicit def unmarshaller[T: Manifest]: Unmarshaller[MultipartFormData] =
    FormDataUnmarshallers.MultipartFormDataUnmarshaller

  override def routes(user: OAuth2Info): Route = {
    
    return uploadBackup(user) ~ executeBackup(user) ~
      downloadBackup(user) ~ getAllBackups(user) ~ deleteAllBackups(user) ~ deleteBackup(user) ~ buildBackup(user) ~
      cleanMetadata(user)
  }

  @Path("/backup/build")
  @ApiOperation(value = "Build a new backup of the metadata",
    notes = "Build a new backup.",
    httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def buildBackup(user: OAuth2Info): Route =
    path(HttpConstant.MetadataPath / "backup" / "build") {
      get {
        complete {
          for {
            response <- supervisor ? BuildBackup
          } yield response match {
            case KongFilesResponse(Success(filesUris)) => filesUris
            case KongFilesResponse(Failure(exception)) => throw exception
            case BackupResponse(Failure(exception)) => throw exception
            case BackupResponse(Success(_)) => throw new ServingCoreException("Backup build not completed")
          }
        }
      }
    }

  @Path("/backup")
  @ApiOperation(value = "Execute one backup.",
    notes = "Execute backup.",
    httpMethod = "POST",
    response = classOf[BackupRequest])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "backup",
      defaultValue = "",
      value = "backup request json",
      dataType = "BackupRequest",
      required = true,
      paramType = "body")))
  def executeBackup(user: OAuth2Info): Route = {
    path(HttpConstant.MetadataPath / "backup") {
      post {
        entity(as[BackupRequest]) { backupRequest =>
          complete {
            for {
              response <- (supervisor ? ExecuteBackup(backupRequest)).mapTo[BackupResponse]
            } yield response match {
              case BackupResponse(Success(_)) => StatusCodes.OK
              case BackupResponse(Failure(exception)) => throw exception
            }
          }
        }
      }
    }
  }

  @Path("/backup")
  @ApiOperation(value = "Upload one backup file.",
    notes = "Creates a backup file in the server filesystem with the uploaded backup file.",
    httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "file",
      value = "The json backup",
      dataType = "file",
      required = true,
      paramType = "formData")
  ))
  def uploadBackup(user: OAuth2Info): Route = {
    path(HttpConstant.MetadataPath / "backup") {
      put {
        entity(as[MultipartFormData]) { form =>
          complete {
            for {
              response <- (supervisor ? UploadBackups(form.fields)).mapTo[KongFilesResponse]
            } yield response match {
              case KongFilesResponse(Success(newFilesUris)) => newFilesUris
              case KongFilesResponse(Failure(exception)) => throw exception
            }
          }
        }
      }
    }
  }

  @Path("/backup/{fileName}")
  @ApiOperation(value = "Download a backup file from directory backups.",
    httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "fileName",
      value = "Name of the backup",
      dataType = "String",
      required = true,
      paramType = "path")
  ))
  def downloadBackup(user: OAuth2Info): Route =
    get {
      pathPrefix(HttpConstant.MetadataPath / "backup") {
        getFromDirectory(
          Try(KongCloudConfig.getDetailConfig.get.getString(AppConstant.BackupsLocation))
            .getOrElse(AppConstant.DefaultBackupsLocation))
      }
    }

  @Path("/backup")
  @ApiOperation(value = "Browse all backups uploaded",
    notes = "Finds all backups.",
    httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def getAllBackups(user: OAuth2Info): Route =
    path(HttpConstant.MetadataPath / "backup") {
      get {
        complete {
          for {
            response <- (supervisor ? ListBackups).mapTo[KongFilesResponse]
          } yield response match {
            case KongFilesResponse(Success(filesUris)) => filesUris
            case KongFilesResponse(Failure(exception)) => throw exception
          }
        }
      }
    }

  @Path("/backup")
  @ApiOperation(value = "Delete all backups uploaded",
    notes = "Delete all backups.",
    httpMethod = "DELETE")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def deleteAllBackups(user: OAuth2Info): Route =
    path(HttpConstant.MetadataPath / "backup") {
      delete {
        complete {
          for {
            response <- (supervisor ? DeleteBackups).mapTo[BackupResponse]
          } yield response match {
            case BackupResponse(Success(_)) => StatusCodes.OK
            case BackupResponse(Failure(exception)) => throw exception
          }
        }
      }
    }

  @Path("/backup/{fileName}")
  @ApiOperation(value = "Delete one backup uploaded or created",
    notes = "Delete one backup.",
    httpMethod = "DELETE")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "fileName",
      value = "Name of the backup",
      dataType = "String",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def deleteBackup(user: OAuth2Info): Route = {
    path(HttpConstant.MetadataPath / "backup" / Segment) { file =>
      delete {
        complete {
          for {
            response <- (supervisor ? DeleteBackup(file)).mapTo[BackupResponse]
          } yield response match {
            case BackupResponse(Success(_)) => StatusCodes.OK
            case BackupResponse(Failure(exception)) => throw exception
          }
        }
      }
    }
  }

  @Path("")
  @ApiOperation(value = "Clean metadata",
    notes = "Clean metadata.",
    httpMethod = "DELETE")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def cleanMetadata(user: OAuth2Info): Route =
    path(HttpConstant.MetadataPath) {
      delete {
        complete {
          for {
            response <- (supervisor ? CleanMetadata).mapTo[BackupResponse]
          } yield response match {
            case BackupResponse(Success(_)) => StatusCodes.OK
            case BackupResponse(Failure(exception)) => throw exception
          }
        }
      }
    }
}
