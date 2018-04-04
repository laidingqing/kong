
package com.kong.eos.serving.api.service.http

import javax.ws.rs.Path

import akka.pattern.ask
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.actor.PluginActor._
import com.kong.eos.serving.api.constants.HttpConstant
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

@Api(value = HttpConstant.PluginsPath, description = "Operations over plugins: now only to upload/download jars.")
trait PluginsHttpService extends BaseHttpService with OauthClient {

  implicit def unmarshaller[T: Manifest]: Unmarshaller[MultipartFormData] =
    FormDataUnmarshallers.MultipartFormDataUnmarshaller

  override def routes(user: OAuth2Info): Route = upload(user) ~
    download(user) ~ getAll(user) ~ deleteAllFiles(user) ~ deleteFile(user)

  @Path("")
  @ApiOperation(value = "Upload a file to plugin directory.",
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
    path(HttpConstant.PluginsPath) {
      put {
        entity(as[MultipartFormData]) { form =>
          complete {
            for {
              response <- (supervisor ? UploadPlugins(form.fields)).mapTo[KongFilesResponse]
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
  @ApiOperation(value = "Download a file from the plugin directory.",
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
        pathPrefix(HttpConstant.PluginsPath) {
        getFromDirectory(
          Try(KongCloudConfig.getDetailConfig.get.getString(AppConstant.PluginsPackageLocation))
            .getOrElse(AppConstant.DefaultPluginsPackageLocation))
      }
    }

  @Path("")
  @ApiOperation(value = "Browse all plugins uploaded",
    notes = "Finds all plugins.",
    httpMethod = "GET")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def getAll(user: OAuth2Info): Route =
    path(HttpConstant.PluginsPath) {
      get {
        complete {
          for {
            response <- (supervisor ? ListPlugins).mapTo[KongFilesResponse]
          } yield response match {
            case KongFilesResponse(Success(filesUris)) => filesUris
            case KongFilesResponse(Failure(exception)) => throw exception
          }
        }
      }
    }

  @Path("")
  @ApiOperation(value = "Delete all plugins uploaded",
    notes = "Delete all plugins.",
    httpMethod = "DELETE")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def deleteAllFiles(user: OAuth2Info): Route =
    path(HttpConstant.PluginsPath) {
      delete {
        complete {
          for {
            response <- (supervisor ? DeletePlugins).mapTo[PluginResponse]
          } yield response match {
            case PluginResponse(Success(_)) => StatusCodes.OK
            case PluginResponse(Failure(exception)) => throw exception
          }
        }
      }
    }

  @Path("/{fileName}")
  @ApiOperation(value = "Delete one plugin uploaded",
    notes = "Delete one plugin.",
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
    path(HttpConstant.PluginsPath / Segment) { file =>
      delete {
        complete {
          for {
            response <- (supervisor ? DeletePlugin(file)).mapTo[PluginResponse]
          } yield response match {
            case PluginResponse(Success(_)) => StatusCodes.OK
            case PluginResponse(Failure(exception)) => throw exception
          }
        }
      }
    }
  }
}
