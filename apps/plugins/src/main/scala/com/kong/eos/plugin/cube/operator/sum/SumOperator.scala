
package com.kong.eos.plugin.cube.operator.sum

import java.io.{Serializable => JSerializable}
import java.io.{Serializable => JSerializable}

import breeze.stats._
import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsNumber}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.types.StructType

import scala.util.Try
/**
  * 求和操作
  * @param name
  * @param schema
  * @param properties
  */
class SumOperator(name: String,
                  val schema: StructType,
                  properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsNumber with Associative {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Double

  override def processReduce(values: Iterable[Option[Any]]): Option[Double] = {
    Try(Option(getDistinctValues(values.flatten.map(value =>
      TypeOp.transformValueByTypeOp(TypeOp.Double, value).asInstanceOf[Double])).sum))
      .getOrElse(Some(Operator.Zero.toDouble))
  }

  def associativity(values: Iterable[(String, Option[Any])]): Option[Double] = {
    val newValues = extractValues(values, None)

    Try(Option(transformValueByTypeOp(returnType, newValues.map(value =>
      TypeOp.transformValueByTypeOp(TypeOp.Double, value).asInstanceOf[Double]).sum)))
      .getOrElse(Some(Operator.Zero.toDouble))
  }
}
