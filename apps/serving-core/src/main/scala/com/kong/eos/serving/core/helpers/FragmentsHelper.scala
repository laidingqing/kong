
package com.kong.eos.serving.core.helpers

import com.kong.eos.serving.core.models.policy.fragment.FragmentType
import com.kong.eos.serving.core.models.policy.fragment.FragmentType.`type`
import com.kong.eos.serving.core.models.policy.PolicyModel
import com.kong.eos.serving.core.models.policy.fragment.{FragmentElementModel, FragmentType}


object FragmentsHelper {

  def populateFragmentFromPolicy(policy: PolicyModel, fragmentType: `type`): Seq[FragmentElementModel] =
    fragmentType match {
      case FragmentType.input =>
        policy.input match {
          case Some(in) => Seq(in.parseToFragment(fragmentType))
          case None => Seq.empty[FragmentElementModel]
        }
      case FragmentType.output =>
        policy.outputs.map(output => output.parseToFragment(fragmentType))
    }
}
