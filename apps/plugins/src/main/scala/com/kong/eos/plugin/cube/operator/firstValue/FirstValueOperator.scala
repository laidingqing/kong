
package com.kong.eos.plugin.cube.operator.firstValue

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.types.StructType

import scala.util.Try


class FirstValueOperator(name: String,
                         val schema: StructType,
                         properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsAny with Associative {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Any

  override def processReduce(values: Iterable[Option[Any]]): Option[Any] =
    Try(Option(values.flatten.head)).getOrElse(None)

  def associativity(values: Iterable[(String, Option[Any])]): Option[Any] = {
    val oldValues = extractValues(values, Option(Operator.OldValuesKey))
    val firstValue = if(oldValues.nonEmpty) oldValues
    else extractValues(values, Option(Operator.NewValuesKey))

    Try(Option(transformValueByTypeOp(returnType, firstValue.head))).getOrElse(None)
  }
}
