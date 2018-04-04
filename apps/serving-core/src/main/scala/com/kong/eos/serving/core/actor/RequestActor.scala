
package com.kong.eos.serving.core.actor

import akka.actor.Actor
import com.kong.eos.serving.core.actor.RequestActor._
import com.kong.eos.serving.core.models.submit.SubmitRequest
import com.kong.eos.serving.core.utils.RequestUtils
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework

class RequestActor() extends Actor with RequestUtils {

  override def receive: Receive = {
    case Create(request) => sender ! createRequest(request)
    case Update(request) => sender ! updateRequest(request)
    case FindAll => sender ! findAllRequests()
    case FindById(id) => sender ! findRequestById(id)
    case DeleteAll => sender ! deleteAllRequests()
    case Delete(id) => sender ! deleteRequest(id)
    case _ => log.info("Unrecognized message in Policy Request Actor")
  }

}

object RequestActor {

  case class Update(request: SubmitRequest)

  case class Create(request: SubmitRequest)

  case class Delete(id: String)

  case object DeleteAll

  case object FindAll

  case class FindById(id: String)
}
