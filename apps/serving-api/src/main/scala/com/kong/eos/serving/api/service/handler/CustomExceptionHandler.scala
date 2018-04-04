
package com.kong.eos.serving.api.service.handler

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.{ErrorModel, KongCloudSerializer}
import org.json4s.jackson.Serialization._
import spray.http.{MediaTypes, StatusCodes}
import spray.routing.ExceptionHandler
import spray.routing.directives.{MiscDirectives, RespondWithDirectives, RouteDirectives}
import spray.util.LoggingContext

/**
 * This exception handler will be used by all our services to return a [ErrorModel] that will be used by the frontend.
 */
object CustomExceptionHandler extends MiscDirectives
with RouteDirectives
with RespondWithDirectives
with SLF4JLogging
with KongCloudSerializer {

  implicit def exceptionHandler(implicit logg: LoggingContext): ExceptionHandler = {
    ExceptionHandler {
      case exception: ServingCoreException =>
        requestUri { uri =>
          log.error(exception.getLocalizedMessage)
          val error = ErrorModel.toErrorModel(exception.getLocalizedMessage)
          respondWithMediaType(MediaTypes.`application/json`) {
            complete(StatusCodes.NotFound, write(error))
          }
        }
      case exception: Throwable =>
        requestUri { uri =>
          log.error(exception.getLocalizedMessage, exception)
          complete((StatusCodes.InternalServerError, write(
            new ErrorModel(ErrorModel.CodeUnknown, Option(exception.getLocalizedMessage).getOrElse("unknown"))
          )))
        }
    }
  }
}