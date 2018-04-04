
package com.kong.eos.sdk.pipeline.aggregation.operator

import java.io.{Serializable => JSerializable}

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp
import com.kong.eos.sdk.pipeline.filter.Filter
import com.kong.eos.sdk.pipeline.schema.{TypeConversions, TypeOp}
import com.kong.eos.sdk.properties.Parameterizable
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import scala.language.reflectiveCalls
import scala.util._

abstract class Operator(name: String,
                        schema: StructType,
                        properties: Map[String, JSerializable]) extends Parameterizable(properties)
  with Ordered[Operator] with TypeConversions with SLF4JLogging with Filter {

  override def operationProps: Map[String, JSerializable] = properties

  def defaultTypeOperation: TypeOp = TypeOp.String

  def key: String = name

  def distinct: Boolean = Try(properties.getString("distinct").toBoolean).getOrElse(false)

  val inputField = properties.getString("inputField", None)

  def processMap(inputFieldsValues: Row): Option[Any]

  def processReduce(values: Iterable[Option[Any]]): Option[Any]

  def returnType: TypeOp = getTypeOperation.getOrElse(defaultTypeOperation)

  def compare(operator: Operator): Int = key compareTo operator.key

  def getDistinctValues[T](values: Iterable[T]): List[T] =
    if (distinct)
      values.toList.distinct
    else values.toList

  def isAssociative: Boolean = this.isInstanceOf[Associative]

  def extractValues(values: Iterable[(String, Option[Any])], filterKey: Option[String]): Iterable[Any] =
    values.flatMap { case (key, value) =>
      filterKey match {
        case Some(filter) => if (key == filter) value else None
        case None => value
      }
    }

  /* Filter Methods */

  def filterInput: Option[String] = properties.getString("filters", None)

  def defaultCastingFilterType: TypeOp = TypeOp.Any
}

object Operator {

  final val ClassSuffix = "Operator"
  final val OldValuesKey = "old"
  final val NewValuesKey = "new"
  final val EmptyString = ""
  final val SpaceSeparator = " "
  final val Zero = 0
  final val UnderscoreSeparator = "_"
}