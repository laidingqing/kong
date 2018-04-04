
package com.kong.eos.plugin.cube.operator.accumulator

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import org.apache.spark.sql.types.StructType
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import scala.util.Try



class AccumulatorOperator(name: String,
                          val schema: StructType,
                          properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsAny with Associative {

  final val Separator = " "

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.ArrayString

  override def processReduce(values: Iterable[Option[Any]]): Option[Any] =
    Try(Option(values.flatten.flatMap(value => {
      value match {
        case value if value.isInstanceOf[Seq[Any]] => value.asInstanceOf[Seq[Any]].map(_.toString)
        case _ => Seq(TypeOp.transformValueByTypeOp(TypeOp.String, value).asInstanceOf[String])
      }
    }))).getOrElse(None)

  def associativity(values: Iterable[(String, Option[Any])]): Option[Any] = {
    val newValues = getDistinctValues(extractValues(values, None).asInstanceOf[Seq[Seq[String]]].flatten)

    Try(Option(transformValueByTypeOp(returnType, newValues))).getOrElse(Option(Seq()))
  }
}