
package com.kong.eos.plugin.transformation.morphline

import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.kitesdk.morphline.api.{Command, Record}

class EventCollector(outputFieldsSchema: Array[StructField]) extends Command {

  @volatile var row: Row = Row.empty

  override def notify(p1: Record): Unit = {}

  def reset(): Unit = row = Row.empty

  //scalastyle:off
  override def getParent: Command = null

  //scalastyle:om

  override def process(recordProcess: Record): Boolean = {
      Option(recordProcess) match {
        case Some(record) =>
          row = Row.fromSeq(outputFieldsSchema.map(field =>
            Option(record.getFirstValue(field.name)) match {
              case Some(value) =>
                TypeOp.transformValueByTypeOp(field.dataType, value.asInstanceOf[Any])
              case None =>
                throw new IllegalStateException(s"Impossible to parse field: ${field.name}.")
            }
          ).toList)
          true
        case None =>
          row = Row.empty
          false
      }
  }
}
