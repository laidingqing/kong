
package com.kong.eos.serving.core.models

import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import com.kong.eos.sdk.properties.JsoneyStringSerializer
import com.kong.eos.serving.core.models.enumerators.PolicyStatusEnum
import com.kong.eos.serving.core.models.policy.PhaseEnum
import org.json4s.ext.EnumNameSerializer
import org.json4s.{DefaultFormats, Formats}

/**
 * Extends this class if you need serialize / unserialize Sparta's enums in any class / object.
 */
trait KongCloudSerializer {

  implicit val json4sJacksonFormats: Formats =
    DefaultFormats +
      new JsoneyStringSerializer() +
      new EnumNameSerializer(PolicyStatusEnum) +
      new EnumNameSerializer(SaveModeEnum) +
      new EnumNameSerializer(PhaseEnum)

}
