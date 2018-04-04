
package com.kong.eos.plugin.cube.operator.range

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Operator, OperatorProcessMapAsNumber}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.types.StructType


class RangeOperator(name: String,
                    val schema: StructType,
                    properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsNumber {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Double

  override def processReduce(values: Iterable[Option[Any]]): Option[Double] = {
    val valuesFiltered = getDistinctValues(values.flatten)
    valuesFiltered.size match {
      case (nz) if nz != 0 =>
        val valuesConverted = valuesFiltered.map(value =>
          TypeOp.transformValueByTypeOp(TypeOp.Double, value).asInstanceOf[Double])
        Some(transformValueByTypeOp(returnType, valuesConverted.max - valuesConverted.min))
      case _ => Some(Operator.Zero.toDouble)
    }
  }
}
