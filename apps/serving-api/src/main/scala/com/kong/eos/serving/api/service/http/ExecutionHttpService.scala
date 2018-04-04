
package com.kong.eos.serving.api.service.http

import akka.pattern.ask
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.actor.RequestActor._
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.ErrorModel
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.core.models.submit.SubmitRequest
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.submit.SubmitRequest
import com.wordnik.swagger.annotations._
import spray.http.{HttpResponse, StatusCodes}
import spray.routing._

import scala.util.{Failure, Success, Try}

@Api(value = HttpConstant.ExecutionsPath, description = "Operations about executions.", position = 0)
trait ExecutionHttpService extends BaseHttpService {

  override def routes(user: OAuth2Info): Route = findAll(user) ~
    update(user) ~ create(user) ~ deleteAll(user) ~ deleteById(user) ~ find(user)

  @ApiOperation(value = "Finds all executions",
    notes = "Returns executions list",
    httpMethod = "GET",
    response = classOf[Seq[SubmitRequest]],
    responseContainer = "List")
  @ApiResponses(
    Array(new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)))
  def findAll(user: OAuth2Info): Route = {
    path(HttpConstant.ExecutionsPath) {
      get {
        complete {
          for {
            response <- (supervisor ? FindAll).mapTo[Try[Seq[SubmitRequest]]]
          } yield response match {
            case Failure(exception) => throw exception
            case Success(executions) => executions
          }
        }
      }
    }
  }

  @ApiOperation(value = "Find a execution from its id.",
    notes = "Find a execution from its id.",
    httpMethod = "GET",
    response = classOf[SubmitRequest])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id",
      value = "id of the policy",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def find(user: OAuth2Info): Route = {
    path(HttpConstant.ExecutionsPath / Segment) { (id) =>
      get {
        complete {
          for {
            response <- (supervisor ? new FindById(id)).mapTo[Try[SubmitRequest]]
          } yield response match {
            case Failure(exception) => throw exception
            case Success(request) => request
          }
        }
      }
    }
  }

  @ApiOperation(value = "Delete all executions",
    notes = "Delete all executions",
    httpMethod = "DELETE")
  @ApiResponses(
    Array(new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)))
  def deleteAll(user: OAuth2Info): Route = {
    path(HttpConstant.ExecutionsPath) {
      delete {
        complete {
          for {
            response <- (supervisor ? DeleteAll).mapTo[Try[_]]
          } yield response match {
            case Failure(exception) => throw exception
            case Success(_) => StatusCodes.OK
          }
        }
      }
    }
  }

  @ApiOperation(value = "Delete a executions by its id",
    notes = "Delete a executions by its id",
    httpMethod = "DELETE")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "id",
      value = "id of the policy",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(
    Array(new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)))
  def deleteById(user: OAuth2Info): Route = {
    path(HttpConstant.ExecutionsPath / Segment) { (id) =>
      delete {
        complete {
          for {
            response <- (supervisor ? Delete(id)).mapTo[Try[_]]
          } yield response match {
            case Failure(exception) => throw exception
            case Success(_) => StatusCodes.OK
          }
        }
      }
    }
  }

  @ApiOperation(value = "Updates a execution.",
    notes = "Updates a execution.",
    httpMethod = "PUT",
    response = classOf[SubmitRequest]
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "execution",
      value = "execution json",
      dataType = "SubmitRequest",
      required = true,
      paramType = "body")))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def update(user: OAuth2Info): Route = {
    path(HttpConstant.ExecutionsPath) {
      put {
        entity(as[SubmitRequest]) { request =>
          complete {
            for {
              response <- (supervisor ? Update(request)).mapTo[Try[SubmitRequest]]
            } yield response match {
              case Success(status) => HttpResponse(StatusCodes.Created)
              case Failure(ex) =>
                val message = "Can't update execution"
                log.error(message, ex)
                throw new ServingCoreException(ErrorModel.toString(
                  ErrorModel(ErrorModel.CodeErrorUpdatingExecution, message)
                ))
            }
          }
        }
      }
    }
  }

  @ApiOperation(value = "Creates a execution",
    notes = "Returns the execution result",
    httpMethod = "POST",
    response = classOf[SubmitRequest])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "execution",
      value = "execution json",
      dataType = "SubmitRequest",
      required = true,
      paramType = "body")))
  @ApiResponses(
    Array(new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)))
  def create(user: OAuth2Info): Route = {
    path(HttpConstant.ExecutionsPath) {
      post {
        entity(as[SubmitRequest]) { request =>
          complete {
            for {
              response <- (supervisor ? Create(request)).mapTo[Try[SubmitRequest]]
            } yield {
              response match {
                case Success(requestCreated) => requestCreated
                case Failure(ex: Throwable) =>
                  val message = "Can't create execution"
                  log.error(message, ex)
                  throw new ServingCoreException(ErrorModel.toString(
                    ErrorModel(ErrorModel.CodeErrorCreatingPolicy, message)
                  ))
              }
            }
          }
        }
      }
    }
  }
}
