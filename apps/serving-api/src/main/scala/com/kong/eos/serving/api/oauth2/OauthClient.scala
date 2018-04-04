package com.kong.eos.serving.api.oauth2

import spray.client.pipelining._
import spray.http._
import spray.http.StatusCodes._
import spray.routing.{Route, _}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import SessionStore._
import OAuth2InfoProtocol._
import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info
import spray.json._
/**
  * provide protect api's access_token validation.
  */
trait OauthClient extends HttpService{
  private final val tokenPrefix = "Bearer "

  implicit val ec: ExecutionContext = ExecutionContext.global
  val configure = new Config

  /**
    * 受保护的接口指令
    * 远程校验access_token 有效情
    * TODO Cookie 保存，减少每次校验。
    */
  val secured: Directive1[String] = {
    if (configure.Enabled) {
      optionalHeaderValueByName("Authorization").map(stripBearerPrefix).flatMap {
        case Some(accessToken:String) => {
          val authInfo = getPrincipal(accessToken)
          authInfo match {
            case Some(info) => {
              //TODO add cookie
              provide(info.toJson.toString())
            }
            case None => complete(Unauthorized)
          }
        }
        case None => complete(Unauthorized)
      }
    } else {
      provide("*")
    }
  }

  private def stripBearerPrefix(token: Option[String]): Option[String] = {
    token.map(_.stripPrefix(tokenPrefix))
  }

  /**
    * check access token & response OAuth2Info
    * @param accessToken
    * @return
    */
  def getPrincipal(accessToken: String): Option[OAuth2Info] = {
    val profileUrl = s"${configure.CheckTokenUrl}?token=$accessToken"
    val info = makeGetRq(profileUrl, accessToken)

    info
  }

  def makeGetRq(url: String, token: String): Option[OAuth2Info] = {
    import OauthClientHelper._

    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    val response = pipeline(Get(url))
    val plainResponse: HttpResponse = Await.result(response, Duration.Inf)
    plainResponse.status match {
      case BadRequest => None
      case OK => parseTokenRs(plainResponse.entity.asString(defaultCharset = HttpCharsets.`UTF-8`))
    }
  }

  val checkAccessToken = path("check_token") {
    get{
      parameter("token") { token =>
        getPrincipal(token) match {
          case Some(info) => complete(OK, info)
          case None => complete(Unauthorized)
        }
      }
    }
  }

  val secRoute = checkAccessToken
}
