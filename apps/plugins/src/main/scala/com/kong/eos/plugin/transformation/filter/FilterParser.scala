
package com.kong.eos.plugin.transformation.filter

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.filter.Filter
import com.kong.eos.sdk.pipeline.schema.TypeOp
import com.kong.eos.sdk.pipeline.schema.TypeOp.TypeOp
import com.kong.eos.sdk.pipeline.transformation.Parser
import com.kong.eos.sdk.properties.ValidatingPropertyMap._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

class FilterParser(order: Integer,
                   inputField: Option[String],
                   outputFields: Seq[String],
                   val schema: StructType,
                   properties: Map[String, JSerializable])
  extends Parser(order, inputField, outputFields, schema, properties) with Filter {

  def filterInput: Option[String] = properties.getString("filters", None)

  def defaultCastingFilterType: TypeOp = TypeOp.Any

  override def parse(row: Row): Seq[Row] =
    applyFilters(row) match {
      case Some(valuesFiltered) =>
        Seq(Row.fromSeq(removeInputField(row)))
      case None => Seq.empty[Row]
    }
}
