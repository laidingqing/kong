
package com.kong.eos.plugin.cube.operator.fullText

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.types.StructType

import scala.util.Try


class FullTextOperator(name: String,
                       val schema: StructType,
                       properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsAny with Associative {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.String

  override def processReduce(values: Iterable[Option[Any]]): Option[String] = {
    Try(Option(values.flatten.map(_.toString).mkString(Operator.SpaceSeparator)))
      .getOrElse(Some(Operator.EmptyString))
  }

  def associativity(values: Iterable[(String, Option[Any])]): Option[String] = {
    val newValues = extractValues(values, None).map(_.toString).mkString(Operator.SpaceSeparator)

    Try(Option(transformValueByTypeOp(returnType, newValues)))
      .getOrElse(Some(Operator.EmptyString))
  }
}
