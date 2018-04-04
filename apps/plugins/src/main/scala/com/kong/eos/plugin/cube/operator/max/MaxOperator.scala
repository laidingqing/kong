
package com.kong.eos.plugin.cube.operator.max

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.types.StructType

import scala.util.Try


/**
  * 最大数据操作
  * @param name
  * @param schema
  * @param properties
  */
class MaxOperator(name: String,
                  val schema: StructType,
                  properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsAny with Associative {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Any

  override def processReduce(values: Iterable[Option[Any]]): Option[Any] =
    Try(Option(getDistinctValues(values.flatten).max)).getOrElse(None)

  def associativity(values: Iterable[(String, Option[Any])]): Option[Any] = {
    val newValues = extractValues(values, None)

    Try(Option(transformValueByTypeOp(returnType, newValues.max))).getOrElse(None)
  }
}
