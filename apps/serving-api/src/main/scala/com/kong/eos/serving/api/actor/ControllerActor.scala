
package com.kong.eos.serving.api.actor

import akka.actor.{ActorContext, ActorRef, _}
import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.headers.{CacheSupport, CorsSupport}
import com.kong.eos.serving.api.service.http._
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.headers.{CacheSupport, CorsSupport}
import com.kong.eos.serving.api.oauth2.OauthClient
import com.kong.eos.serving.api.service.handler.CustomExceptionHandler._
import com.kong.eos.serving.api.service.http._
import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.constants.{AkkaConstant, AppConstant}
import com.kong.eos.serving.core.models.KongCloudSerializer
import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info
import com.kong.eos.serving.api.constants.HttpConstant
import com.kong.eos.serving.api.headers.{CacheSupport, CorsSupport}
import com.kong.eos.serving.api.oauth2.OauthClient
import com.kong.eos.serving.api.service.http._
import com.typesafe.config.Config
import org.apache.curator.framework.CuratorFramework
import spray.http.StatusCodes._
import spray.routing._

import scala.util.{Properties, Try}

class ControllerActor(actorsMap: Map[String, ActorRef]) extends HttpServiceActor
  with SLF4JLogging
  with KongCloudSerializer
  with CorsSupport
  with CacheSupport
  with OauthClient {

  override implicit def actorRefFactory: ActorContext = context

  val serviceRoutes: ServiceRoutes = new ServiceRoutes(actorsMap, context)

  val oauthConfig: Option[Config] = KongCloudConfig.getOauth2Config
  val enabledSecurity: Boolean = Try(oauthConfig.get.getString("enable").toBoolean).getOrElse(false)
  val cookieName: String = Try(oauthConfig.get.getString("cookieName")).getOrElse(AppConstant.DefaultOauth2CookieName)

  def receive: Receive = runRoute(handleExceptions(exceptionHandler)(getRoutes))

  def getRoutes: Route = cors{
    secRoute ~ staticRoutes ~ dynamicRoutes
  }

  private def redirectToRoot: Route =
  path(HttpConstant.KongCloudRootPath){
    get{
      requestUri{ uri =>
        redirect(s"${uri.toString}/", Found)
      }
    }
  }

  private def staticRoutes: Route = {
    webRoutes
  }

  private def dynamicRoutes: Route = {
    import com.kong.eos.serving.api.oauth2.OAuth2InfoProtocol._
    secured { userAuth =>
      val user: Option[OAuth2Info] = userAuth
      user match {
        case Some(parsedUser: OAuth2Info) =>
          authorize(parsedUser.isAuthorized(enabledSecurity)) {
            allServiceRoutes(parsedUser)
          }
        case None => complete(Unauthorized)
      }
    }
  }

  private def allServiceRoutes(user: OAuth2Info): Route = {
    serviceRoutes.fragmentRoute(user) ~ serviceRoutes.policyContextRoute(user) ~
      serviceRoutes.executionRoute(user) ~ serviceRoutes.policyRoute(user) ~ serviceRoutes.appStatusRoute(user) ~
      serviceRoutes.pluginsRoute(user) ~ serviceRoutes.driversRoute(user) ~ serviceRoutes.swaggerRoute ~
      serviceRoutes.metadataRoute(user) ~ serviceRoutes.serviceInfoRoute(user) ~ serviceRoutes.configRoute(user)
  }

  private def webRoutes: Route =
    get {
      pathPrefix(HttpConstant.SwaggerPath) {
        pathEndOrSingleSlash {
          getFromResource("swagger-ui/index.html")
        }
      } ~ getFromResourceDirectory("swagger-ui") ~
        pathPrefix("") {
          pathEndOrSingleSlash {
            getFromResource("classes/web/index.html")
          }
        } ~ getFromResourceDirectory("classes/web") ~
        pathPrefix("") {
          pathEndOrSingleSlash {
            getFromResource("web/index.html")
          }
        } ~ getFromResourceDirectory("web")
    }
}

class ServiceRoutes(actorsMap: Map[String, ActorRef], context: ActorContext) {

  def fragmentRoute(user: OAuth2Info): Route = fragmentService.routes(user)

  def policyRoute(user: OAuth2Info): Route = policyService.routes(user)

  def policyContextRoute(user: OAuth2Info): Route = policyContextService.routes(user)

  def executionRoute(user: OAuth2Info): Route = executionService.routes(user)

  def appStatusRoute(user: OAuth2Info): Route = appStatusService.routes(user)

  def pluginsRoute(user: OAuth2Info): Route = pluginsService.routes(user)

  def driversRoute(user: OAuth2Info): Route = driversService.routes(user)

  def configRoute(user: OAuth2Info): Route = configService.routes(user)

  def metadataRoute(user: OAuth2Info): Route = metadataService.routes(user)

  def serviceInfoRoute(user: OAuth2Info): Route = serviceInfoService.routes(user)

  def swaggerRoute: Route = swaggerService.routes

  def getMarathonLBPath: Option[String] = {
    val marathonLB_host = Properties.envOrElse("MARATHON_APP_LABEL_HAPROXY_0_VHOST","")
    val marathonLB_path = Properties.envOrElse("MARATHON_APP_LABEL_HAPROXY_0_PATH", "")

    if(marathonLB_host.nonEmpty && marathonLB_path.nonEmpty)
      Some("https://" + marathonLB_host + marathonLB_path)
    else None
  }

  private val fragmentService = new FragmentHttpService {
    implicit val actors = actorsMap
    override val supervisor = actorsMap(AkkaConstant.FragmentActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val policyService = new PolicyHttpService {
    implicit val actors = actorsMap
    override val supervisor = actorsMap(AkkaConstant.PolicyActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val policyContextService = new PolicyContextHttpService {
    implicit val actors = actorsMap
    override val supervisor = actorsMap(AkkaConstant.LauncherActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val executionService = new ExecutionHttpService {
    implicit val actors = actorsMap
    override val supervisor = actorsMap(AkkaConstant.ExecutionActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val appStatusService = new AppStatusHttpService {
    override implicit val actors: Map[String, ActorRef] = actorsMap
    override val supervisor: ActorRef = context.self
    override val actorRefFactory: ActorRefFactory = context
  }

  private val pluginsService = new PluginsHttpService {
    override implicit val actors: Map[String, ActorRef] = actorsMap
    override val supervisor: ActorRef = actorsMap(AkkaConstant.PluginActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val driversService = new DriverHttpService {
    override implicit val actors: Map[String, ActorRef] = actorsMap
    override val supervisor: ActorRef = actorsMap(AkkaConstant.DriverActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val configService = new ConfigHttpService {
    override implicit val actors: Map[String, ActorRef] = actorsMap
    override val supervisor: ActorRef = actorsMap(AkkaConstant.ConfigActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val metadataService = new MetadataHttpService {
    override implicit val actors: Map[String, ActorRef] = actorsMap
    override val supervisor: ActorRef = actorsMap(AkkaConstant.MetadataActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val serviceInfoService = new InfoServiceHttpService {
    override implicit val actors: Map[String, ActorRef] = actorsMap
    override val supervisor: ActorRef = actorsMap(AkkaConstant.MetadataActorName)
    override val actorRefFactory: ActorRefFactory = context
  }

  private val swaggerService = new SwaggerService {
    override implicit def actorRefFactory: ActorRefFactory = context
    override def baseUrl: String = getMarathonLBPath match {
      case Some(marathonLBpath) => marathonLBpath
      case None => "/"
    }
  }
}
