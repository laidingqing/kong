
package com.kong.eos.serving.api.service.http

import javax.ws.rs.Path

import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.core.models.dto.OAuth2._
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.helpers.InfoHelper
import com.kong.eos.serving.core.models.ErrorModel
import com.kong.eos.serving.core.models.dto.LoggedUser
import com.kong.eos.serving.core.models.info.AppInfo
import com.wordnik.swagger.annotations._
import spray.routing._

import scala.util.Try

@Api(value = HttpConstant.AppInfoPath, description = "Operations about information of server")
trait InfoServiceHttpService extends BaseHttpService {

  override def routes(user: OAuth2Info): Route = getInfo

  @Path("")
  @ApiOperation(value = "Return the server info",
    notes = "Return the server info",
    httpMethod = "GET")
  @ApiResponses(Array(new ApiResponse(code = 200, message = "Return the server info", response = classOf[AppInfo])))
  def getInfo: Route = {
    path(HttpConstant.AppInfoPath) {
      get {
        complete {
          Try(InfoHelper.getAppInfo).getOrElse(
            throw new ServingCoreException(ErrorModel.toString(
              new ErrorModel(ErrorModel.CodeUnknown, s"Imposible to extract server information")
            ))
          )
        }
      }
    }
  }
}
