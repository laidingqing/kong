
package com.kong.eos.plugin.cube.operator.median

import java.io.{Serializable => JSerializable}
import breeze.linalg._
import breeze.stats._
import breeze.linalg.DenseVector
import com.kong.eos.sdk.pipeline.aggregation.operator.{Operator, OperatorProcessMapAsNumber}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.types.StructType

/**
  * 中位数操作
  * @param name
  * @param schema
  * @param properties
  */
class MedianOperator(name: String,
                     val schema: StructType,
                     properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsNumber {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.Double

  override def processReduce(values: Iterable[Option[Any]]): Option[Double] = {
    val valuesFiltered = getDistinctValues(values.flatten)
    valuesFiltered.size match {
      case (nz) if (nz != 0) => Some(transformValueByTypeOp(returnType,
        median(DenseVector(valuesFiltered.map(_.asInstanceOf[Number].doubleValue()).toArray))))
      case _ => Some(Operator.Zero.toDouble)
    }
  }
}
