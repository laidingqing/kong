
package com.kong.eos.sdk.pipeline.autoCalculations

case class AutoCalculatedField(
                                fromNotNullFields: Option[FromNotNullFields],
                                fromPkFields: Option[FromPkFields],
                                fromFields: Option[FromFields],
                                fromFixedValue: Option[FromFixedValue]
                             )
