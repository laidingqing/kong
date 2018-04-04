
package com.kong.eos.serving.core.models.policy.trigger

import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.writer.WriterModel

case class TriggerModel(name: String,
                        sql: String,
                        writer: WriterModel,
                        overLast: Option[String] = None,
                        configuration: Map[String, JsoneyString] = Map(),
                        computeEvery: Option[String] = None
                       )
