
package com.kong.eos.serving.core.models.policy

import com.kong.eos.sdk.properties.JsoneyString
import com.kong.eos.serving.core.models.policy.writer.WriterModel

case class RawDataModel(dataField: String,
                        timeField: String,
                        writer: WriterModel,
                        configuration: Map[String, JsoneyString] = Map())

