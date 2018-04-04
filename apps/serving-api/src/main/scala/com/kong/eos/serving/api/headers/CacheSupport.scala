
package com.kong.eos.serving.api.headers

import spray.http.HttpHeaders._
import spray.http._
import spray.routing._

trait CacheSupport {

  this: HttpService =>

  private val CacheControlHeader = `Cache-Control`(CacheDirectives.`no-cache`)

  def noCache[T]: Directive0 = mapRequestContext {
    ctx => ctx.withHttpResponseHeadersMapped { headers =>
      CacheControlHeader :: headers
    }
  }
}