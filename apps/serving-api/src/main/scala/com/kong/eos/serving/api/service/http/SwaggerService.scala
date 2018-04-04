
package com.kong.eos.serving.api.service.http

import com.gettyimages.spray.swagger.SwaggerHttpService
import com.wordnik.swagger.model.ApiInfo

import scala.reflect.runtime.universe._

trait SwaggerService extends SwaggerHttpService {

  override def apiTypes: Seq[Type] = Seq(
    typeOf[FragmentHttpService],
    typeOf[PolicyHttpService],
    typeOf[PolicyContextHttpService],
    typeOf[PluginsHttpService],
    typeOf[DriverHttpService],
    typeOf[AppStatusHttpService],
    typeOf[ExecutionHttpService],
    typeOf[InfoServiceHttpService],
    typeOf[ConfigHttpService],
    typeOf[MetadataHttpService]
  )

  override def apiVersion: String = "1.0"

  // let swagger-ui determine the host and port
  override def docsPath: String = "api-docs"

  override def apiInfo: Option[ApiInfo] = Some(ApiInfo(
    "KongCloud",
    "A real time aggregation engine full spark based.",
    "",
    "laidingqing@gmail.com",
    "Apache V2",
    "http://www.apache.org/licenses/LICENSE-2.0"
  ))

}
