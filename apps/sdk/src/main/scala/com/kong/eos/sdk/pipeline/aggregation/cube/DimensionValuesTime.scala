
package com.kong.eos.sdk.pipeline.aggregation.cube

case class DimensionValuesTime(cube: String,
                               dimensionValues: Seq[DimensionValue],
                               timeConfig: Option[TimeConfig] = None)
