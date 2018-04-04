
package com.kong.eos.plugin.cube.operator.entityCount

import java.io.{Serializable => JSerializable}

import org.apache.spark.sql.types.StructType


class OperatorEntityCountMock(name: String, schema: StructType, properties: Map[String, JSerializable])
  extends OperatorEntityCount(name, schema, properties) {

  override def processReduce(values: Iterable[Option[Any]]): Option[Any] = values.head

}
