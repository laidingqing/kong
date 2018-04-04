
package com.kong.eos.plugin.cube.operator.count

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.aggregation.operator.{Associative, Operator, OperatorProcessMapAsAny}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import org.apache.spark.sql.types.StructType
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import org.apache.spark.sql.Row
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import scala.util.Try

/**
  * 计数操作
  * @param name
  * @param schema
  * @param properties
  */
class CountOperator(name: String, val schema: StructType, properties: Map[String, JSerializable])
  extends Operator(name, schema, properties) with Associative {

  val distinctFields = parseDistinctFields

  override val defaultTypeOperation = TypeOp.Long

  override def processMap(inputFieldsValues: Row): Option[Any] = {
    applyFilters(inputFieldsValues).flatMap(filteredFields => distinctFields match {
      case None => Option(CountOperator.One.toLong)
      case Some(fields) => Option(fields.map(field => filteredFields.getOrElse(field, CountOperator.NullValue))
        .mkString(Operator.UnderscoreSeparator).toString)
    })
  }

  override def processReduce(values: Iterable[Option[Any]]): Option[Long] = {
    Try {
      val longList = distinctFields match {
        case None => values.flatten.map(value => value.asInstanceOf[Number].longValue())
        case Some(fields) => values.flatten.toList.distinct.map(value => CountOperator.One.toLong)
      }
      Option(longList.sum)
    }.getOrElse(Option(Operator.Zero.toLong))
  }

  def associativity(values: Iterable[(String, Option[Any])]): Option[Long] = {
    val newValues = extractValues(values, None)
      .map(value => TypeOp.transformValueByTypeOp(TypeOp.Long, value).asInstanceOf[Long]).sum

    Try(Option(transformValueByTypeOp(returnType, newValues)))
      .getOrElse(Option(Operator.Zero.toLong))
  }

  //FIXME: We should refactor this code
  private def parseDistinctFields: Option[Seq[String]] = {
    val distinct = properties.getString("distinctFields", None)
    if (distinct.isDefined && !distinct.get.isEmpty)
      Option(distinct.get.split(Operator.UnderscoreSeparator))
    else None
  }
}

object CountOperator {

  final val NullValue = "None"
  final val One = 1
}
