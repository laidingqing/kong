
package com.kong.eos.plugin.cube.operator.entityCount

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.types.StructType
import com.kong.eos.sdk._
import org.apache.spark.sql.types.StructType
import scala.util.Try

class EntityCountOperator(name: String,
                          schema: StructType,
                          properties: Map[String, JSerializable])
  extends OperatorEntityCount(name, schema, properties) with Associative {

  final val Some_Empty = Some(Map("" -> 0L))

  override val defaultTypeOperation = TypeOp.MapStringLong

  override def processReduce(values: Iterable[Option[Any]]): Option[Seq[String]] =
    Try(Option(values.flatten.flatMap(_.asInstanceOf[Seq[String]]).toSeq))
      .getOrElse(None)

  def associativity(values: Iterable[(String, Option[Any])]): Option[Map[String, Long]] = {
    val oldValues = extractValues(values, Option(Operator.OldValuesKey))
      .flatMap(_.asInstanceOf[Map[String, Long]]).toList
    val newValues = applyCount(extractValues(values, Option(Operator.NewValuesKey))
      .flatMap(value => {
        value match {
          case value if value.isInstanceOf[Seq[String]] => value.asInstanceOf[Seq[String]]
          case _ => List(TypeOp.transformValueByTypeOp(TypeOp.String, value).asInstanceOf[String])
        }
      }).toList).toList
    val wordCounts = applyCountMerge(oldValues ++ newValues)

    Try(Option(transformValueByTypeOp(returnType, wordCounts)))
      .getOrElse(Option(Map()))
  }

  private def applyCount(values: List[String]): Map[String, Long] =
    values.groupBy((word: String) => word).mapValues(_.length.toLong)

  private def applyCountMerge(values: List[(String, Long)]): Map[String, Long] =
    values.groupBy { case (word, count) => word }.mapValues {
      listValues => listValues.map { case (key, value) => value }.sum
    }
}


