
package com.kong.eos.serving.core.models.policy.cube

import com.kong.eos.serving.core.models.policy.trigger.TriggerModel
import com.kong.eos.serving.core.models.policy.writer.WriterModel

case class CubeModel(name: String,
                     dimensions: Seq[DimensionModel],
                     operators: Seq[OperatorModel],
                     writer: WriterModel,
                     triggers: Seq[TriggerModel] = Seq.empty[TriggerModel],
                     avoidNullValues: Boolean = true
                    )

