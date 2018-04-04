package com.kong.eos.sdk.pipeline.aggregation.cube

import com.kong.eos.sdk.pipeline.schema.{TypeConversions, TypeOp}
import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp

trait DimensionType extends TypeConversions {

  /**
   * When writing to the cube at some address, the address will have one coordinate for each
   * dimension in the cube, for example (time: 348524388, location: portland). For each
   * dimension, for each precision type within that dimension, the dimensionType must transform the
   * input data into the precision that should be used to store the data.
   *
   * @param value Used to generate the different precisions
   * @return Map with all generated precisions and a sequence with all values
   */
  def precisionValue(keyName: String, value: Any): (Precision, Any)

  /**
   * All precisions supported into this dimensionType
   *
   * @return Sequence of Precisions
   */
  def precision(keyName : String): Precision

  def properties: Map[String, JSerializable] = Map()

  override def defaultTypeOperation: TypeOp = TypeOp.String

}

object DimensionType {

  final val IdentityName = "identity"
  final val IdentityFieldName = "identityField"
  final val TimestampName = "timestamp"
  final val DefaultDimensionClass = "Default"

  def getIdentity(typeOperation: Option[TypeOp], default: TypeOp): Precision =
    new Precision(IdentityName, typeOperation.getOrElse(default))

  def getIdentityField(typeOperation: Option[TypeOp], default: TypeOp): Precision =
    new Precision(IdentityFieldName, typeOperation.getOrElse(default))

  def getTimestamp(typeOperation: Option[TypeOp], default: TypeOp): Precision =
    new Precision(TimestampName, typeOperation.getOrElse(default))

}