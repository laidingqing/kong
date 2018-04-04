
package com.kong.eos.serving.core.models.policy.fragment

import com.kong.eos.serving.core.exception.ServingCoreException
import com.kong.eos.serving.core.models.ErrorModel
import com.kong.eos.serving.core.models.policy.PolicyElementModel

/**
 * A fragmentElementDto represents a piece of policy that will be composed with other fragments before.
 *
 * @param fragmentType that could be inputs/outputs/parsers
 * @param name that will be used as an identifier of the fragment.
 * @param element with all config parameters of the fragment.
 */
case class FragmentElementModel(id: Option[String] = None,
                                var userId: String = "",
                                fragmentType: String,
                                name: String,
                                description: String,
                                shortDescription: String,
                                element:PolicyElementModel){

  def getIdIfEquals: (FragmentElementModel) => Option[String] = {
    currentFragment => this.equals(currentFragment) match {
      case true => currentFragment.id
      case false => throw new ServingCoreException(ErrorModel.toString(
        new ErrorModel(ErrorModel.CodeExistsFragmentWithName,
          s"Fragment of type ${this.fragmentType} with name ${this.name} exists.")))
    }
  }

  def setUserId(id: String) ={
    this.userId = id
  }
}

object FragmentType extends Enumeration {
  type `type` = Value
  val InputValue = "input"
  val OutputValue = "output"
  val input = Value(InputValue)
  val output = Value(OutputValue)
  val AllowedTypes = Seq(input, output)
}
