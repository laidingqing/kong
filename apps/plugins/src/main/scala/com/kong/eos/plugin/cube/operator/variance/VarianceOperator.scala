
package com.kong.eos.plugin.cube.operator.variance

import java.io.{Serializable => JSerializable}
import java.io.{Serializable => JSerializable}
import breeze.stats._
import com.kong.eos.sdk.pipeline.aggregation.operator.{Operator, OperatorProcessMapAsNumber}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.types.StructType

/**
  * 方差操作
  * @param name
  * @param schema
  * @param properties
  */
class VarianceOperator(name: String,
                       val schema: StructType,
                       properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsNumber {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Double

  override def processReduce(values: Iterable[Option[Any]]): Option[Double] = {
    val valuesFiltered = getDistinctValues(values.flatten)
    valuesFiltered.size match {
      case (nz) if nz != 0 =>
        Some(transformValueByTypeOp(returnType, variance(valuesFiltered.map(value =>
          TypeOp.transformValueByTypeOp(TypeOp.Double, value).asInstanceOf[Double]))))
      case _ => Some(Operator.Zero.toDouble)
    }
  }
}
