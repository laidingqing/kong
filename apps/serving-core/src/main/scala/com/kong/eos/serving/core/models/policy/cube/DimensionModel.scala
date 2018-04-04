
package com.kong.eos.serving.core.models.policy.cube

import com.kong.eos.sdk.pipeline.aggregation.cube.DimensionType

case class DimensionModel(name: String,
                          field: String,
                          precision: String = DimensionType.IdentityName,
                          `type`: String = DimensionType.DefaultDimensionClass,
                          computeLast: Option[String] = None,
                          configuration: Option[Map[String, String]] = None) {

}
