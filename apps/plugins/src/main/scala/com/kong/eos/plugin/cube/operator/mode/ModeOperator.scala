
package com.kong.eos.plugin.cube.operator.mode

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.types.StructType

import scala.util.Try

class ModeOperator(name: String,
                   val schema: StructType,
                   properties: Map[String, JSerializable]) extends Operator(name, schema, properties)
with OperatorProcessMapAsAny {

  val inputSchema = schema

  override val defaultTypeOperation = TypeOp.ArrayString

  override def processReduce(values: Iterable[Option[Any]]): Option[Any] = {
    val tupla = values.groupBy(x => x).mapValues(_.size)
    if (tupla.nonEmpty) {
      val max = tupla.values.max
      Try(Some(transformValueByTypeOp(returnType, tupla.filter(_._2 == max).flatMap(tuple => tuple._1)))).get
    } else Some(List())
  }
}
