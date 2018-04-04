
package com.kong.eos.plugin.cube.operator.totalEntityCount

import java.io.{Serializable => JSerializable}

import com.kong.eos.plugin.cube.operator.entityCount.OperatorEntityCount
import java.io.{Serializable => JSerializable}

import breeze.stats._
import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsNumber}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.types.StructType

import scala.util.Try


class TotalEntityCountOperator(name: String,
                               schema: StructType,
                               properties: Map[String, JSerializable])
  extends OperatorEntityCount(name, schema, properties) with Associative {

  final val Some_Empty = Some(0)

  override val defaultTypeOperation = TypeOp.Int

  override def processReduce(values: Iterable[Option[Any]]): Option[Int] =
    Try(Option(values.flatten.map(value => {
      value match {
        case value if value.isInstanceOf[Seq[_]] => getDistinctValues(value.asInstanceOf[Seq[_]]).size
        case _ => TypeOp.transformValueByTypeOp(TypeOp.Int, value).asInstanceOf[Int]
      }
    }).sum)).getOrElse(Some_Empty)

  def associativity(values: Iterable[(String, Option[Any])]): Option[Int] = {
    val newValues =
      extractValues(values, None).map(value => TypeOp.transformValueByTypeOp(TypeOp.Int, value).asInstanceOf[Int]).sum

    Try(Option(transformValueByTypeOp(returnType, newValues)))
      .getOrElse(Some_Empty)
  }
}


