
package com.kong.eos.plugin.cube.field.defaultField

import com.kong.eos.sdk.pipeline.aggregation.cube.{DimensionType, Precision}
import com.kong.eos.sdk.pipeline.schema.TypeOp
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class DefaultFieldTest extends WordSpecLike with Matchers {

  val defaultDimension: DefaultField = new DefaultField(Map("typeOp" -> "int"))

  "A DefaultDimension" should {
    "In default implementation, get one precisions for a specific time" in {
      val precision: (Precision, Any) = defaultDimension.precisionValue("", "1".asInstanceOf[Any])

      precision._2 should be(1)

      precision._1.id should be(DimensionType.IdentityName)
    }

    "The precision must be int" in {
      defaultDimension.precision(DimensionType.IdentityName).typeOp should be(TypeOp.Int)
    }
  }
}
