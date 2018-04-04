
package com.kong.eos.plugin.cube.field.defaultField

import java.io.{Serializable => JSerializable}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionType, Precision}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._

case class DefaultField(props: Map[String, JSerializable], override val defaultTypeOperation : TypeOp)
  extends DimensionType with JSerializable with SLF4JLogging {

  def this(defaultTypeOperation : TypeOp) {
    this(Map(), defaultTypeOperation)
  }

  def this(props: Map[String, JSerializable]) {
    this(props,  TypeOp.String)
  }

  def this() {
    this(Map(), TypeOp.String)
  }

  override val operationProps: Map[String, JSerializable] = props

  override val properties: Map[String, JSerializable] = props

  override def precisionValue(keyName: String, value: Any): (Precision, Any) = {
    val precision = DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
    (precision, TypeOp.transformValueByTypeOp(precision.typeOp, value))
  }

  override def precision(keyName: String): Precision =
    DimensionType.getIdentity(getTypeOperation, defaultTypeOperation)
}
