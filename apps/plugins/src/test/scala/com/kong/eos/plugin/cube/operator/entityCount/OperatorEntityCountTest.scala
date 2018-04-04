
package com.kong.eos.plugin.cube.operator.entityCount

import java.io.{Serializable => JSerializable}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}


@RunWith(classOf[JUnitRunner])
class OperatorEntityCountTest extends WordSpec with Matchers {

  "EntityCount" should {
    val props = Map(
      "inputField" -> "inputField".asInstanceOf[JSerializable],
      "split" -> ",".asInstanceOf[JSerializable])
    val schema = StructType(Seq(StructField("inputField", StringType)))
    val entityCount = new OperatorEntityCountMock("op1", schema, props)
    val inputFields = Row("hello,bye")

    "Return the associated precision name" in {
      val expected = Option(Seq("hello", "bye"))
      val result = entityCount.processMap(inputFields)
      result should be(expected)
    }

    "Return empty list" in {
      val expected = None
      val result = entityCount.processMap(Row())
      result should be(expected)
    }
  }
}
