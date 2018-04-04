
package com.kong.eos.serving.api.service.http

import javax.ws.rs.Path

import akka.pattern.ask
import com.kong.eos.serving.api.actor.ConfigActor.{ConfigResponse, FindAll}
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.api.constants.HttpConstant
import com.wordnik.swagger.annotations.{Api, ApiOperation, ApiResponse, ApiResponses}
import spray.routing.Route


@Api(value = HttpConstant.ConfigPath, description = "Operations on Sparta Configuration")
trait ConfigHttpService extends BaseHttpService with KongCloudSerializer {

  override def routes(user: OAuth2Info): Route = getAll(user)

  @Path("")
  @ApiOperation(value = "Retrieve all frontend configuration settings",
    notes = "Returns configuration value for frontend",
    httpMethod = "GET")
  @ApiResponses(
    Array(new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)))
  def getAll(user: OAuth2Info): Route = {
    path(HttpConstant.ConfigPath) {
      get {
        complete {
          for {
            response <- (supervisor ? FindAll).mapTo[ConfigResponse]
          } yield response match {
            case ConfigResponse(config) => config
          }
        }
      }
    }
  }

}
