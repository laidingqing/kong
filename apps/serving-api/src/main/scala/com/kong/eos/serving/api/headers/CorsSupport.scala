
package com.kong.eos.serving.api.headers

import spray.http.HttpHeaders._
import spray.http.HttpMethods._
import spray.http.{AllOrigins, HttpMethod, HttpMethods, HttpResponse}
import spray.routing._

trait CorsSupport {

  this: HttpService =>

  private final val MaxAge = 1728000

  private val AllowOriginHeader = `Access-Control-Allow-Origin`(AllOrigins)
  private val OptionsCorsHeaders = List(
    `Access-Control-Allow-Headers`(
      "Origin, X-Requested-With, Content-Type, Accept, Accept-Encoding, Accept-Language, Host, Referer, User-Agent"),
    `Access-Control-Max-Age`(MaxAge))

  def cors[T]: Directive0 = mapRequestContext {
    ctx => ctx.withRouteResponseHandling({
      case Rejected(x) if (ctx.request.method.equals(HttpMethods.OPTIONS) &&
        !x.filter(_.isInstanceOf[MethodRejection]).isEmpty) => {
          val allowedMethods: List[HttpMethod] = x.filter(_.isInstanceOf[MethodRejection]).map(rejection => {
            rejection.asInstanceOf[MethodRejection].supported
          })
          ctx.complete(HttpResponse().withHeaders(
            `Access-Control-Allow-Methods`(OPTIONS, allowedMethods: _*) :: AllowOriginHeader :: OptionsCorsHeaders
          ))
        }
      }).withHttpResponseHeadersMapped { headers =>
      AllowOriginHeader :: headers
    }
  }
}
