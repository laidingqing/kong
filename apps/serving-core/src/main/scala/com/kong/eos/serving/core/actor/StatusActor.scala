
package com.kong.eos.serving.core.actor

import akka.actor.{Actor, _}
import com.kong.eos.serving.core.actor.StatusActor._
import com.kong.eos.serving.core.utils.PolicyStatusUtils
import com.kong.eos.serving.core.models.policy.PolicyStatusModel
import com.kong.eos.serving.core.utils.{ClusterListenerUtils, PolicyStatusUtils}
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache

import scala.util.Try

class StatusActor() extends Actor with PolicyStatusUtils with ClusterListenerUtils {

  //scalastyle:off cyclomatic.complexity
  override def receive: Receive = {
    case Create(policyStatus) => sender ! ResponseStatus(createStatus(policyStatus))
    case Update(policyStatus) => sender ! ResponseStatus(updateStatus(policyStatus))
    case ClearLastError(id) => sender ! clearLastError(id)
    case FindAll => sender ! findAllStatuses()
    case FindById(id) => sender ! ResponseStatus(findStatusById(id))
    case DeleteAll => sender ! ResponseDelete(deleteAllStatuses())
    case AddListener(name, callback) => addListener(name, callback)
    case AddClusterListeners => addClusterListeners(findAllStatuses(), context)
    case Delete(id) => sender ! ResponseDelete(deleteStatus(id))
    case _ => log.info("Unrecognized message in Policy Status Actor")
  }

  //scalastyle:on cyclomatic.complexity
}

object StatusActor {

  case class Update(policyStatus: PolicyStatusModel)

  case class Create(policyStatus: PolicyStatusModel)

  case class AddListener(name: String, callback: (PolicyStatusModel, NodeCache) => Unit)

  case class Delete(id: String)

  case object DeleteAll

  case object FindAll

  case class FindById(id: String)

  case class ResponseStatus(policyStatus: Try[PolicyStatusModel])

  case class ResponseDelete(value: Try[_])

  case class ClearLastError(id: String)

  case object AddClusterListeners

}