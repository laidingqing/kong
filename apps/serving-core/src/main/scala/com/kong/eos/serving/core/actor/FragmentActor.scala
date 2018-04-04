
package com.kong.eos.serving.core.actor

import akka.actor.Actor
import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info
import com.kong.eos.serving.core.models.policy.ResponsePolicy
import com.kong.eos.serving.core.utils.OAuth2InfoUtils
import com.kong.eos.serving.core.actor.FragmentActor.{Response, ResponseFragment, ResponseFragments}
import com.kong.eos.serving.core.actor.FragmentActor._
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.ErrorModel
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import com.kong.eos.serving.core.models.policy.{PolicyModel, ResponsePolicy}
import com.kong.eos.serving.core.utils.{FragmentUtils, OAuth2InfoUtils}
import com.mongodb.casbah.MongoClient
import org.apache.curator.framework.CuratorFramework
import spray.httpx.Json4sJacksonSupport

import scala.util.Try

class FragmentActor() extends Actor with Json4sJacksonSupport with FragmentUtils with OAuth2InfoUtils{

  //scalastyle:off
  override def receive: Receive = {
    case FindAllFragments(user) => findAll(user)
    case FindByType(user, fragmentType) => findByType(fragmentType, user)
    case FindByTypeAndId(user, fragmentType, id) => findByTypeAndId(user, fragmentType, id)
    case FindByTypeAndName(user, fragmentType, name) => findByTypeAndName(fragmentType, name.toLowerCase(), user)
    case DeleteAllFragments(user) => deleteAll(user)
    case DeleteByType(user, fragmentType) => deleteByType(user, fragmentType)
    case DeleteByTypeAndId(user, fragmentType, id) => deleteByTypeAndId(fragmentType, id)
    case DeleteByTypeAndName(user, fragmentType, name) => deleteByTypeAndName(fragmentType, name)
    case Create(fragment) => create(fragment)
    case Update(user, fragment) => update(user, fragment)
    case PolicyWithFragments(policy) => policyWithFragments(policy)
    case _ => log.info("Unrecognized message in Fragment Actor")
  }

  //scalastyle:on

  def findAll(user: OAuth2Info): Unit = {
    sender ! ResponseFragments(Try(findAllFragments(user.userId)))
  }

  def findByType(fragmentType: String, user: OAuth2Info): Unit =
    sender ! ResponseFragments(Try(findFragmentsByType(fragmentType, user.userId)))

  def findByTypeAndId(user: OAuth2Info, fragmentType: String, id: String): Unit =
    sender ! ResponseFragment(Try(findFragmentByTypeAndId(fragmentType, id)))

  def findByTypeAndName(fragmentType: String, name: String, user: OAuth2Info): Unit =
    sender ! ResponseFragment(Try(findFragmentByTypeAndName(user.userId, fragmentType, name)
      .getOrElse(throw new ServingCoreException(ErrorModel.toString(new ErrorModel(
        ErrorModel.CodeNotExistsPolicyWithName, s"No fragment of type $fragmentType with name $name"))))))

  def create(fragment: FragmentElementModel): Unit =
    sender ! ResponseFragment(Try(createFragment(fragment)))

  def update(user: OAuth2Info, fragment: FragmentElementModel): Unit =
    sender ! Response(Try(updateFragment(user.userId, fragment)))

  def deleteAll(user: OAuth2Info): Unit =
    sender ! ResponseFragments(Try(deleteAllFragments(user.userId)))

  def deleteByType(user: OAuth2Info, fragmentType: String): Unit =
    sender ! Response(Try(deleteFragmentsByType(user.userId, fragmentType)))

  def deleteByTypeAndId(fragmentType: String, id: String): Unit =
    sender ! Response(Try(deleteFragmentById(id)))

  def deleteByTypeAndName(fragmentType: String, name: String): Unit =
    sender ! Response(Try(deleteFragmentByTypeAndName(fragmentType, name)))

  def policyWithFragments(policyModel: PolicyModel) : Unit =
    sender ! ResponsePolicy(Try(getPolicyWithFragments(policyModel)))

}

object FragmentActor {

  case class PolicyWithFragments(policy: PolicyModel)

  case class Create(fragment: FragmentElementModel)

  case class Update(user: OAuth2Info, fragment: FragmentElementModel)

  case class FindAllFragments(user: OAuth2Info)

  case class FindByType(user: OAuth2Info, fragmentType: String)

  case class FindByTypeAndId(user: OAuth2Info, fragmentType: String, id: String)

  case class FindByTypeAndName(user: OAuth2Info, fragmentType: String, name: String)

  case class DeleteAllFragments(user: OAuth2Info)

  case class DeleteByType(user: OAuth2Info, fragmentType: String)

  case class DeleteByTypeAndId(user: OAuth2Info, fragmentType: String, id: String)

  case class DeleteByTypeAndName(user: OAuth2Info, fragmentType: String, name: String)

  case class ResponseFragment(fragment: Try[FragmentElementModel])

  case class ResponseFragments(fragments: Try[Seq[FragmentElementModel]])

  case class Response(status: Try[_])

}