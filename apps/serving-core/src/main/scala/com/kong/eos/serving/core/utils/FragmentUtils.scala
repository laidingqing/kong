
package com.kong.eos.serving.core.utils

import java.util.UUID

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.serving.core.constants.AppConstant
import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.helpers.FragmentsHelper
import com.kong.eos.serving.core.models.policy.fragment.FragmentType._
import com.kong.eos.serving.core.models.policy.fragment.{FragmentElementModel, FragmentType}
import com.kong.eos.serving.core.models.policy.{PolicyElementModel, PolicyModel}
import com.kong.eos.serving.core.models.{ErrorModel, KongCloudSerializer}
import org.apache.curator.framework.CuratorFramework
import org.json4s.jackson.Serialization._

import scala.collection.JavaConversions
import scala.util.{Failure, Success, Try}
import com.kong.eos.serving.core.constants.AppConstant._
import com.kong.eos.serving.core.models.dto.OAuth2.OAuth2Info
import com.kong.eos.serving.core.services.FragmentCasbahService
import com.mongodb.casbah.MongoClient

trait FragmentUtils extends SLF4JLogging with KongCloudSerializer{

  def findAllFragments(userId: String): List[FragmentElementModel] = {
    val entries = FragmentCasbahService().findAllFragment(userId) match {
      case Success(x) => x
      case Failure(exception) => throw exception
    }
    entries
  }

  def findFragmentsByType(fragmentType: String, userId: String): List[FragmentElementModel] = {

    val entries = FragmentCasbahService().findFragmentsByType(fragmentType, userId) match {
      case Success(x) => x
      case Failure(exception) => List.empty[FragmentElementModel]
    }
    entries

  }

  def findFragmentByTypeAndId(fragmentType: String, id: String): FragmentElementModel = {
    val entry = FragmentCasbahService().findFragmentByTypeAndId(fragmentType, id) match {
      case Success(x) => x
      case Failure(exception) => exception
    }
    return entry match {
      case Some(x: FragmentElementModel) => x
      case _ => throw new ServingCoreException(ErrorModel.toString(
        new ErrorModel(ErrorModel.CodeNotExistsFragmentWithId, s"Fragment type: $fragmentType and id: $id not exists")))
    }
  }

  def findFragmentByTypeAndName(userId: String, fragmentType: String, name: String): Option[FragmentElementModel] =
    findFragmentsByType(fragmentType, userId).find(fragment => fragment.name == name)

  def createFragment(fragment: FragmentElementModel): FragmentElementModel =
    findFragmentByTypeAndName(fragment.userId, fragment.fragmentType, fragment.name.toLowerCase)
      .getOrElse(createNewFragment(fragment))

  def updateFragment(userId: String, fragment: FragmentElementModel): FragmentElementModel = {
    val newFragment = fragment.copy(name = fragment.name.toLowerCase)
//    curatorFramework.setData().forPath(
//      s"${fragmentPathType(newFragment.fragmentType)}/${fragment.id.get}", write(newFragment).getBytes)
    newFragment



  }

  def deleteAllFragments(userId: String): List[FragmentElementModel] = {
    val fragmentsFound = findAllFragments(userId)
    fragmentsFound.foreach(fragment => {
      val id = fragment.id.getOrElse {
        throw new ServingCoreException(ErrorModel.toString(
          new ErrorModel(ErrorModel.CodeErrorDeletingAllFragments, s"Fragment without id: ${fragment.name}.")))
      }
      deleteFragmentById(id)
    })
    fragmentsFound
  }

  def deleteFragmentsByType(userId: String, fragmentType: String): Unit = {
    FragmentCasbahService().deleteFragmentByType(userId, fragmentType)
  }

  def deleteFragmentById(id: String): Unit = {
    FragmentCasbahService().deleteFragmentById(id)
  }

  def deleteFragmentByTypeAndName(fragmentType: String, name: String): Unit = {
    //val fragmentFound = findFragmentByTypeAndName(fragmentType, name)
//    if (fragmentFound.isDefined && fragmentFound.get.id.isDefined) {
//      val id = fragmentFound.get.id.get
//      val fragmentLocation = s"${fragmentPathType(fragmentType)}/$id"
//      if (CuratorFactoryHolder.existsPath(fragmentLocation))
//        curatorFramework.delete().forPath(fragmentLocation)
//      else throw new ServingCoreException(ErrorModel.toString(new ErrorModel(
//        ErrorModel.CodeNotExistsFragmentWithId, s"Fragment type: $fragmentType and id: $id not exists")))
//    } else {
//      throw new ServingCoreException(ErrorModel.toString(new ErrorModel(
//        ErrorModel.CodeExistsFragmentWithName, s"Fragment without id: $name.")))
//    }

    throw new ServingCoreException(ErrorModel.toString(new ErrorModel(
      ErrorModel.CodeExistsFragmentWithName, s"Fragment without id: $name.")))
  }

  /* PRIVATE METHODS */

  private def createNewFragment(fragment: FragmentElementModel): FragmentElementModel = {
    val newFragment = fragment.copy(
      id = Option(UUID.randomUUID.toString),
      name = fragment.name.toLowerCase
    )
    val entry = FragmentCasbahService().createFragment(newFragment) match {
      case Success(x) => x
      case Failure(exception) => throw exception
    }

    newFragment
  }

  private def fragmentPathType(fragmentType: String): String = {
    fragmentType match {
      case "input" => s"$FragmentsPath/input"
      case "output" => s"$FragmentsPath/output"
      case _ => throw new IllegalArgumentException("The fragment type must be input|output")
    }
  }

  /* POLICY METHODS */

  def getPolicyWithFragments(policy: PolicyModel): PolicyModel = {
    val policyWithFragments = parseFragments(fillFragments(policy))
    if (policyWithFragments.fragments.isEmpty) {
      val input = FragmentsHelper.populateFragmentFromPolicy(policy, FragmentType.input)
      val outputs = FragmentsHelper.populateFragmentFromPolicy(policy, FragmentType.output)
      policyWithFragments.copy(fragments = input ++ outputs)
    } else policyWithFragments
  }

  private def parseFragments(apConfig: PolicyModel): PolicyModel = {
    val fragmentInputs = getFragmentFromType(apConfig.fragments, FragmentType.input)
    val fragmentOutputs = getFragmentFromType(apConfig.fragments, FragmentType.output)

    apConfig.copy(
      input = Some(getCurrentInput(fragmentInputs, apConfig.input)),
      outputs = getCurrentOutputs(fragmentOutputs, apConfig.outputs))
  }

  private def fillFragments(apConfig: PolicyModel): PolicyModel = {
    val currentFragments = apConfig.fragments.flatMap(fragment => {
      fragment.id match {
        case Some(id) =>
          Try(findFragmentByTypeAndId(fragment.fragmentType, id)).toOption
        case None => findFragmentByTypeAndName(fragment.userId, fragment.fragmentType, fragment.name)
      }
    })
    apConfig.copy(fragments = currentFragments)
  }

  private def getFragmentFromType(fragments: Seq[FragmentElementModel], fragmentType: `type`)
  : Seq[FragmentElementModel] = {
    fragments.flatMap(fragment =>
      if (FragmentType.withName(fragment.fragmentType) == fragmentType) Some(fragment) else None)
  }

  private def getCurrentInput(fragmentsInputs: Seq[FragmentElementModel],
                              inputs: Option[PolicyElementModel]): PolicyElementModel = {

    if (fragmentsInputs.isEmpty && inputs.isEmpty) {
      throw new IllegalStateException("It is mandatory to define one input in the policy.")
    }

    if ((fragmentsInputs.size > 1) ||
      (fragmentsInputs.size == 1 && inputs.isDefined &&
        ((fragmentsInputs.head.name != inputs.get.name) ||
          (fragmentsInputs.head.element.configuration.getOrElse(
            AppConstant.CustomTypeKey, fragmentsInputs.head.element.`type`) !=
            inputs.get.configuration.getOrElse(AppConstant.CustomTypeKey, inputs.get.`type`))))) {
      throw new IllegalStateException("Only one input is allowed in the policy.")
    }

    if (fragmentsInputs.isEmpty) inputs.get else fragmentsInputs.head.element.copy(name = fragmentsInputs.head.name)
  }

  private def getCurrentOutputs(fragmentsOutputs: Seq[FragmentElementModel],
                                outputs: Seq[PolicyElementModel]): Seq[PolicyElementModel] = {

    val outputsTypesNames = fragmentsOutputs.map(fragment =>
      (fragment.element.configuration.getOrElse(AppConstant.CustomTypeKey, fragment.element.`type`), fragment.name))

    val outputsNotIncluded = for {
      output <- outputs
      outputType = output.configuration.getOrElse(AppConstant.CustomTypeKey, output.`type`)
      outputTypeName = (outputType, output.name)
    } yield if (outputsTypesNames.contains(outputTypeName)) None else Some(output)

    fragmentsOutputs.map(fragment => fragment.element.copy(name = fragment.name)) ++ outputsNotIncluded.flatten
  }
}