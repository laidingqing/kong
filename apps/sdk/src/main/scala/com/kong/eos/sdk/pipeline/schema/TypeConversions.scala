package com.kong.eos.sdk.pipeline.schema

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.pipeline.aggregation.cube.Precision
import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp

trait TypeConversions {

  final val TypeOperationName = "typeOp"

  def defaultTypeOperation: TypeOp

  def operationProps: Map[String, JSerializable] = Map()

  def getTypeOperation: Option[TypeOp] = getResultType(TypeOperationName)

  def getTypeOperation(typeOperation: String): Option[TypeOp] =
    if (!typeOperation.isEmpty && operationProps.contains(typeOperation)) getResultType(typeOperation)
    else getResultType(TypeOperationName)

  def getPrecision(precision: String, typeOperation: Option[TypeOp]): Precision =
    new Precision(precision, typeOperation.getOrElse(defaultTypeOperation))

  def getResultType(typeOperation: String): Option[TypeOp] =
    if (!typeOperation.isEmpty) {
      operationProps.get(typeOperation).flatMap(operation =>
        Option(operation).flatMap(op => Some(TypeOp.getTypeOperationByName(op.toString, defaultTypeOperation))))
    } else None
}
