
package com.kong.eos.plugin.cube.field.datetime

import java.io.{Serializable => JSerializable}
import java.util.Date

import akka.event.slf4j.SLF4JLogging
import com.kong.eos.plugin.cube.field.datetime.DateTimeField._
import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionType, Precision}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp._
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import com.kong.eos.sdk.utils.AggregationTime
import org.joda.time.DateTime

case class DateTimeField(props: Map[String, JSerializable], override val defaultTypeOperation: TypeOp)
  extends DimensionType with JSerializable with SLF4JLogging {

  def this(defaultTypeOperation: TypeOp) {
    this(Map.empty[String, JSerializable], defaultTypeOperation)
  }

  def this(props: Map[String, JSerializable]) {
    this(props, TypeOp.Timestamp)
  }

  def this() {
    this(Map.empty[String, JSerializable], TypeOp.Timestamp)
  }

  override val operationProps: Map[String, JSerializable] = props

  override val properties: Map[String, JSerializable] = props ++ {
    if (props.getString(AggregationTime.GranularityPropertyName, None).isEmpty)
      Map(AggregationTime.GranularityPropertyName -> AggregationTime.DefaultGranularity)
    else Map.empty[String, JSerializable]
  }

  override def precision(keyName: String): Precision = {
    if (AggregationTime.precisionsMatches(keyName).nonEmpty) getPrecision(keyName, getTypeOperation(keyName))
    else TimestampPrecision
  }

  @throws(classOf[ClassCastException])
  override def precisionValue(keyName: String, value: Any): (Precision, Any) =
    try {
      val precisionKey = precision(keyName)
      (precisionKey,
        getPrecision(
          TypeOp.transformValueByTypeOp(TypeOp.DateTime, value).asInstanceOf[DateTime],
          precisionKey,
          properties
        ))
    }
    catch {
      case cce: ClassCastException =>
        log.error("Error parsing " + value + " .")
        throw cce
    }

  private def getPrecision(value: DateTime, precision: Precision, properties: Map[String, JSerializable]): Any = {
    TypeOp.transformValueByTypeOp(precision.typeOp,
      AggregationTime.truncateDate(value, precision match {
        case t if t == TimestampPrecision => if (properties.contains(AggregationTime.GranularityPropertyName))
          properties.get(AggregationTime.GranularityPropertyName).get.toString
        else AggregationTime.DefaultGranularity
        case _ => precision.id
      })).asInstanceOf[Any]
  }
}

object DateTimeField {

  final val TimestampPrecision = DimensionType.getTimestamp(Some(TypeOp.Timestamp), TypeOp.Timestamp)
}
