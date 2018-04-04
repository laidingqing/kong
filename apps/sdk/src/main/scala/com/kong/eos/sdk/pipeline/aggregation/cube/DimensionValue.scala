
package com.kong.eos.sdk.pipeline.aggregation.cube

case class DimensionValue(dimension: Dimension, value: Any) extends Ordered[DimensionValue] {

  def getNameDimension: String = dimension.name

  def compare(dimensionValue: DimensionValue): Int = dimension compareTo dimensionValue.dimension
}
