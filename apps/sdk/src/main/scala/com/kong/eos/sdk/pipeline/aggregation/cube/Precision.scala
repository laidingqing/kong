package com.kong.eos.sdk.pipeline.aggregation.cube

import java.io.{Serializable => JSerializable}
import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp

case class Precision(id: String, typeOp: TypeOp, properties: Map[String, JSerializable]) {

  def this(id: String, typeOp: TypeOp) {
    this(id, typeOp, Map())
  }
}
