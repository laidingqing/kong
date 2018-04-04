
package com.kong.eos.serving.core.models.policy

import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.fragment.FragmentElementModel
import com.kong.eos.serving.core.models.policy.fragment.FragmentType.`type`

case class PolicyElementModel(name: String, `type`: String, configuration: Map[String, JsoneyString] = Map()) {

  def parseToFragment(fragmentType: `type`): FragmentElementModel = {
    FragmentElementModel(
      id = None,
      fragmentType = fragmentType.toString,
      name = name,
      description = "",
      shortDescription = "",
      element = this)
  }
}
