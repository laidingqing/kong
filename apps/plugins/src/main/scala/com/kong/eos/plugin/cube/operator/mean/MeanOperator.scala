
package com.kong.eos.plugin.cube.operator.mean

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Operator, OperatorProcessMapAsNumber}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.types.StructType

/**
  * 平均数操作
  * @param name
  * @param schema
  * @param properties
  */
class MeanOperator(name: String, val schema: StructType, properties: Map[String, JSerializable])
  extends Operator(name, schema, properties) with OperatorProcessMapAsNumber {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Double

  override def processReduce(values: Iterable[Option[Any]]): Option[Double] = {
    val distinctValues = getDistinctValues(values.flatten)
    distinctValues.size match {
      case (nz) if nz != 0 => Some(transformValueByTypeOp(returnType,
        distinctValues.map(_.asInstanceOf[Number].doubleValue()).sum / distinctValues.size))
      case _ => Some(Operator.Zero.toDouble)
    }
  }
}
