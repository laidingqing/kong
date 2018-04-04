
package com.kong.eos.serving.api.service.http

import java.io.File
import javax.ws.rs.Path

import akka.pattern.ask
import com.kong.eos.serving.api.actor.PolicyActor._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.actor.LauncherActor.Launch
import com.kong.eos.serving.core.actor.{FragmentActor, StatusActor}
import com.kong.eos.serving.core.actor.StatusActor.ResponseDelete
import com.kong.eos.serving.core.constants.AkkaConstant
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import com.kong.eos.serving.core.models.policy.{PolicyModel, PolicyValidator, ResponsePolicy}
import com.kong.eos.serving.core.models.{ErrorModel, KongCloudSerializer}
import com.kong.eos.serving.api.constants.HttpConstant
import com.wordnik.swagger.annotations._
import org.json4s.jackson.Serialization.write
import spray.http.HttpHeaders.`Content-Disposition`
import spray.http.{HttpResponse, StatusCodes}
import spray.routing._

import scala.concurrent.Await
import scala.util.{Failure, Success, Try}

@Api(value = HttpConstant.PolicyPath, description = "Operations over policies.")
trait PolicyHttpService extends BaseHttpService with KongCloudSerializer {

  override def routes(user: OAuth2Info): Route =
    find(user) ~ findAll(user) ~ findByFragment(user) ~ create(user) ~
      update(user) ~ remove(user) ~ run(user) ~ download(user) ~ findByName(user) ~
      removeAll(user) ~ deleteCheckpoint(user)

  @Path("/find/{id}")
  @ApiOperation(value = "Find a policy from its id.",
    notes = "Find a policy from its id.",
    httpMethod = "GET",
    response = classOf[PolicyModel])
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
    path(HttpConstant.PolicyPath / "find" / Segment) { (id) =>
      get {
        complete {
          for {
            response <- (supervisor ? Find(id)).mapTo[ResponsePolicy]
          } yield response match {
            case ResponsePolicy(Failure(exception)) =>
              throw exception
            case ResponsePolicy(Success(policy)) => policy
          }
        }
      }
    }
  }

  @Path("/findByName/{name}")
  @ApiOperation(value = "Find a policy from its name.",
    notes = "Find a policy from its name.",
    httpMethod = "GET",
    response = classOf[PolicyModel])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "name",
      value = "name of the policy",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def findByName(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / "findByName" / Segment) { (name) =>
      get {
        complete {
          for {
            response <- (supervisor ? FindByName(name)).mapTo[ResponsePolicy]
          } yield response match {
            case ResponsePolicy(Failure(exception)) =>
              throw exception
            case ResponsePolicy(Success(policy)) => policy
          }
        }
      }
    }
  }

  @Path("/fragment/{fragmentType}/{id}")
  @ApiOperation(value = "Finds policies that contains a fragment.",
    notes = "Finds policies that contains a fragment.",
    httpMethod = "GET",
    response = classOf[PolicyModel])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "fragmentType",
      value = "type of fragment (input/output)",
      dataType = "string",
      required = true,
      paramType = "path"),
    new ApiImplicitParam(name = "id",
      value = "id of the fragment",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def findByFragment(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / "fragment" / Segment / Segment) { (fragmentType, id) =>
      get {
        complete {
          for {
            response <- (supervisor ? FindByFragment(fragmentType, id)).mapTo[ResponsePolicies]
          } yield response match {
            case ResponsePolicies(Failure(exception)) => throw exception
            case ResponsePolicies(Success(policies)) => policies
          }
        }
      }
    }
  }

  @Path("/all")
  @ApiOperation(value = "Finds all policies.",
    notes = "Finds all policies.",
    httpMethod = "GET",
    response = classOf[PolicyModel])
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def findAll(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / "all") {
      get {
        complete {
          for {
            response <- (supervisor ? FindAll()).mapTo[ResponsePolicies]
          } yield response match {
            case ResponsePolicies(Failure(exception)) => throw exception
            case ResponsePolicies(Success(policies)) => policies
          }
        }
      }
    }
  }

  @ApiOperation(value = "Creates a policy.",
    notes = "Creates a policy.",
    httpMethod = "POST",
    response = classOf[PolicyModel])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "policy",
      defaultValue = "",
      value = "policy json",
      dataType = "AggregationPoliciesModel",
      required = true,
      paramType = "body")))
  def create(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath) {
      post {
        entity(as[PolicyModel]) { policy =>
          complete {
            val fragmentActor = actors.getOrElse(AkkaConstant.FragmentActorName, throw new ServingCoreException
            (ErrorModel.toString(ErrorModel(ErrorModel.CodeUnknown, s"Error getting fragmentActor"))))
            for {
              parsedP <- (fragmentActor ? FragmentActor.PolicyWithFragments(policy)).mapTo[ResponsePolicy]
            } yield parsedP match {
              case ResponsePolicy(Failure(exception)) =>
                throw exception
              case ResponsePolicy(Success(policyParsed)) =>
                PolicyValidator.validateDto(policyParsed)
                for {
                  response <- (supervisor ? Create(policy)).mapTo[ResponsePolicy]
                } yield response match {
                  case ResponsePolicy(Failure(exception)) => throw exception
                  case ResponsePolicy(Success(policy)) => policy
                }
            }
          }
        }
      }
    }
  }

  @ApiOperation(value = "Updates a policy.",
    notes = "Updates a policy.",
    httpMethod = "PUT")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "policy",
      defaultValue = "",
      value = "policy json",
      dataType = "AggregationPoliciesModel",
      required = true,
      paramType = "body")))
  def update(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath) {
      put {
        entity(as[PolicyModel]) { policy =>
          complete {
            PolicyValidator.validateDto(policy)
            for {
              response <- (supervisor ? Update(policy)).mapTo[ResponsePolicy]
            } yield response match {
              case ResponsePolicy(Failure(exception)) => throw exception
              case ResponsePolicy(Success(policy)) => HttpResponse(StatusCodes.OK)
            }
          }
        }
      }
    }
  }

  @ApiOperation(value = "Deletes all policies.",
    notes = "Deletes all policies.",
    httpMethod = "DELETE")
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def removeAll(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath) {
      delete {
        complete {
          val statusActor = actors(AkkaConstant.StatusActorName)
          for {
            policies <- (supervisor ? DeleteAll()).mapTo[ResponsePolicies]
          } yield policies match {
            case ResponsePolicies(Failure(exception)) =>
              throw exception
            case ResponsePolicies(Success(policies: Seq[PolicyModel])) =>
              for {
                response <- (statusActor ? StatusActor.DeleteAll).mapTo[ResponseDelete]
              } yield response match {
                case ResponseDelete(Success(_)) => StatusCodes.OK
                case ResponseDelete(Failure(exception)) => throw exception
              }
          }
        }
      }
    }
  }

  @ApiOperation(value = "Deletes a policy from its id.",
    notes = "Deletes a policy from its id.",
    httpMethod = "DELETE")
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
  def remove(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / Segment) { (id) =>
      delete {
        complete {
          for {
            response <- (supervisor ? Delete(id)).mapTo[Response]
          } yield response match {
            case Response(Failure(ex)) => throw ex
            case Response(Success(_)) => StatusCodes.OK
          }
        }
      }
    }
  }

  @Path("/checkpoint/{name}")
  @ApiOperation(value = "Delete checkpoint associated to one policy from its name.",
    notes = "Delete checkpoint associated to one policy from its name.",
    httpMethod = "DELETE",
    response = classOf[Result])
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "name",
      value = "name of the policy",
      dataType = "string",
      required = true,
      paramType = "path")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)
  ))
  def deleteCheckpoint(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / "checkpoint" / Segment) { (name) =>
      delete {
        complete {
          for {
            responsePolicy <- (supervisor ? FindByName(name)).mapTo[ResponsePolicy]
          } yield responsePolicy match {
            case ResponsePolicy(Failure(exception)) => throw exception
            case ResponsePolicy(Success(policy)) => for {
              response <- (supervisor ? DeleteCheckpoint(policy)).mapTo[Response]
            } yield response match {
              case Response(Failure(ex)) => throw ex
              case Response(Success(_)) => Result("Checkpoint deleted from policy: " + policy.name)
            }
          }
        }
      }
    }
  }

  @Path("/run/{id}")
  @ApiOperation(value = "Runs a policy from its name.",
    notes = "Runs a policy from its name.",
    httpMethod = "GET",
    response = classOf[Result])
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
  def run(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / "run" / Segment) { (id) =>
      get {
        complete {
          for (result <- supervisor ? Find(id)) yield result match {
            case ResponsePolicy(Failure(exception)) => throw exception
            case ResponsePolicy(Success(policy)) =>
              val launcherActor = actors(AkkaConstant.LauncherActorName)
              for {
                response <- (launcherActor ? Launch(policy)).mapTo[Try[PolicyModel]]
              } yield response match {
                case Failure(ex) => throw ex
                case Success(policyModel) => Result("Launched policy with name " + policyModel.name)
              }
          }
        }
      }
    }
  }

  @Path("/download/{id}")
  @ApiOperation(value = "Downloads a policy from its id.",
    notes = "Downloads a policy from its id.",
    httpMethod = "GET",
    response = classOf[PolicyModel])
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
  def download(user: OAuth2Info): Route = {
    path(HttpConstant.PolicyPath / "download" / Segment) { (id) =>
      get {
        val future = supervisor ? Find(id)
        Await.result(future, timeout.duration) match {
          case ResponsePolicy(Failure(exception)) =>
            throw exception
          case ResponsePolicy(Success(policy)) =>
            PolicyValidator.validateDto(policy)
            val tempFile = File.createTempFile(s"${policy.id.get}-${policy.name}-", ".json")
            tempFile.deleteOnExit()
            respondWithHeader(`Content-Disposition`("attachment", Map("filename" -> s"${policy.name}.json"))) {
              scala.tools.nsc.io.File(tempFile).writeAll(
                write(policy.copy(fragments = Seq.empty[FragmentElementModel])))
              getFromFile(tempFile)
            }
        }
      }
    }
  }

  case class Result(message: String, desc: Option[String] = None)

}
