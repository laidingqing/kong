
package com.kong.eos.sdk.pipeline.aggregation.cube

case class Dimension(name: String, field: String, precisionKey: String, dimensionType: DimensionType)
  extends Ordered[Dimension] {

  val precision = dimensionType.precision(precisionKey)

  def getNamePrecision: String = precision.id match {
    case DimensionType.IdentityName => field
    case _ => precision.id
  }

  def compare(dimension: Dimension): Int = name compareTo dimension.name
}

object Dimension {

  final val FieldClassSuffix = "Field"
}