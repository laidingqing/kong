
package com.kong.eos.serving.api.service.http

import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.ErrorModel
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.api.constants.HttpConstant
import com.mongodb.casbah.MongoClient
import com.wordnik.swagger.annotations._
import org.apache.curator.framework.CuratorFramework
import spray.routing._

@Api(value = HttpConstant.AppStatus, description = "Operations about sparta status.")
trait AppStatusHttpService extends BaseHttpService {

  override def routes(user: OAuth2Info): Route = checkStatus

  @ApiOperation(value = "Check Cloud status depends to Zookeeper connexion",
    notes = "Returns Cloud status",
    httpMethod = "GET",
    response = classOf[String],
    responseContainer = "List")
  @ApiResponses(
    Array(new ApiResponse(code = HttpConstant.NotFound,
      message = HttpConstant.NotFoundMessage)))
  def checkStatus: Route = {
    path(HttpConstant.AppStatus) {
      get {
        complete {
           "OK"
        }
      }
    }
  }
}
