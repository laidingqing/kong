
package com.kong.eos.plugin.cube.operator.lastValue

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.types.StructType

import scala.util.Try

class LastValueOperator(name: String,
                        val schema: StructType,
                        properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsAny with Associative {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Any

  override def processReduce(values: Iterable[Option[Any]]): Option[Any] =
    Try(Option(values.flatten.last)).getOrElse(None)

  def associativity(values: Iterable[(String, Option[Any])]): Option[Any] = {
    val newValues = extractValues(values, Option(Operator.NewValuesKey))
    val lastValue = if(newValues.nonEmpty) newValues
    else extractValues(values, Option(Operator.OldValuesKey))

    Try(Option(transformValueByTypeOp(returnType, lastValue.last))).getOrElse(None)
  }
}
