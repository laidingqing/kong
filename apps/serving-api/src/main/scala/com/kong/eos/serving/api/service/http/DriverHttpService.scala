
package com.kong.eos.serving.api.service.http

import javax.ws.rs.Path

import akka.pattern.ask
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.DriverActor._
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.api.oauth2.OauthClient
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.core.models.files.KongFilesResponse
import com.kong.eos.serving.api.constants.HttpConstant
import com.wordnik.swagger.annotations._
import spray.http._
import spray.httpx.unmarshalling.{FormDataUnmarshallers, Unmarshaller}
import spray.routing.Route

import scala.util.{Failure, Success, Try}

@Api(value = HttpConstant.DriverPath, description = "Operations over plugins: now only to upload/download jars.")
trait DriverHttpService extends BaseHttpService with OauthClient {

  implicit def unmarshaller[T: Manifest]: Unmarshaller[MultipartFormData] =
    FormDataUnmarshallers.MultipartFormDataUnmarshaller

  override def routes(user: OAuth2Info): Route = upload(user) ~
    download(user) ~ getAll(user) ~ deleteAllFiles(user) ~ deleteFile(user)

  @Path("")
  @ApiOperation(value = "Upload a file to driver directory.",
    notes = "Creates a file in the server filesystem with the uploaded jar.",
    httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "file",
      value = "The jar",
      dataType = "file",
      required = true,
      paramType = "formData")
  ))
  def upload(user: OAuth2Info): Route = {
    path(HttpConstant.DriverPath) {
      put {
        entity(as[MultipartFormData]) { form =>
          complete {
            for {
              response <- (supervisor ? UploadDrivers(form.fields)).mapTo[KongFilesResponse]
            } yield response match {
              case KongFilesResponse(Success(newFilesUris)) => newFilesUris
              case KongFilesResponse(Failure(exception)) => throw exception
            }
          }
        }
      }
    }
  }

  @Path("/{fileName}")
  @ApiOperation(value = "Download a file from the driver directory.",
    httpMethod = "GET")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "fileName",
      value = "Name of the jar",
      dataType = "String",
      required = true,
      paramType = "path")
  ))
  def download(user: OAuth2Info): Route =
    get {
      pathPrefix(HttpConstant.DriverPath) {
        getFromDirectory(
          Try(KongCloudConfig.getDetailConfig.get.getString(AppConstant.DriverPackageLocation))
            .getOrElse(AppConstant.DefaultDriverPackageLocation))
      }
    }

  @Path("")
  @ApiOperation(value = "Browse all drivers uploaded",
    notes = "Finds all drivers.",
    httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def getAll(user: OAuth2Info): Route =
    path(HttpConstant.DriverPath) {
      get {
        complete {
          for {
            response <- (supervisor ? ListDrivers).mapTo[KongFilesResponse]
          } yield response match {
            case KongFilesResponse(Success(filesUris)) => filesUris
            case KongFilesResponse(Failure(exception)) => throw exception
          }
        }
      }
    }

  @Path("")
  @ApiOperation(value = "Delete all drivers uploaded",
    notes = "Delete all drivers.",
    httpMethod = "DELETE")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def deleteAllFiles(user: OAuth2Info): Route =
    path(HttpConstant.DriverPath) {
      delete {
        complete {
          for {
            response <- (supervisor ? DeleteDrivers).mapTo[DriverResponse]
          } yield response match {
            case DriverResponse(Success(_)) => StatusCodes.OK
            case DriverResponse(Failure(exception)) => throw exception
          }
        }
      }
    }

  @Path("/{fileName}")
  @ApiOperation(value = "Delete one driver uploaded",
    notes = "Delete one driver.",
    httpMethod = "DELETE")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "fileName",
      value = "Name of the jar",
      dataType = "String",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def deleteFile(user: OAuth2Info): Route = {
    path(HttpConstant.DriverPath / Segment) { file =>
      delete {
        complete {
          for {
            response <- (supervisor ? DeleteDriver(file)).mapTo[DriverResponse]
          } yield response match {
            case DriverResponse(Success(_)) => StatusCodes.OK
            case DriverResponse(Failure(exception)) => throw exception
          }
        }
      }
    }
  }
}
