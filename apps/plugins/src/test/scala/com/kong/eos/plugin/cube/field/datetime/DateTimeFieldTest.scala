
package com.kong.eos.plugin.cube.field.datetime

import java.io.{Serializable => JSerializable}
import java.util.Date

import com.kong.eos.sdk.pipeline.schema.TypeOp
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class DateTimeFieldTest extends WordSpecLike with Matchers {

  val dateTimeDimension = new DateTimeField(Map("second" -> "long", "minute" -> "date", "typeOp" -> "datetime"))

  "A DateTimeDimension" should {
    "In default implementation, get 6 dimensions for a specific time" in {
      val newDate = new Date()
      val precision5s =
        dateTimeDimension.precisionValue("5s", newDate.asInstanceOf[JSerializable])
      val precision10s =
        dateTimeDimension.precisionValue("10s", newDate.asInstanceOf[JSerializable])
      val precision15s =
        dateTimeDimension.precisionValue("15s", newDate.asInstanceOf[JSerializable])
      val precisionSecond =
        dateTimeDimension.precisionValue("second", newDate.asInstanceOf[JSerializable])
      val precisionMinute =
        dateTimeDimension.precisionValue("minute", newDate.asInstanceOf[JSerializable])
      val precisionHour =
        dateTimeDimension.precisionValue("hour", newDate.asInstanceOf[JSerializable])
      val precisionDay =
        dateTimeDimension.precisionValue("day", newDate.asInstanceOf[JSerializable])
      val precisionMonth =
        dateTimeDimension.precisionValue("month", newDate.asInstanceOf[JSerializable])
      val precisionYear =
        dateTimeDimension.precisionValue("year", newDate.asInstanceOf[JSerializable])

      precision5s._1.id should be("5s")
      precision10s._1.id should be("10s")
      precision15s._1.id should be("15s")
      precisionSecond._1.id should be("second")
      precisionMinute._1.id should be("minute")
      precisionHour._1.id should be("hour")
      precisionDay._1.id should be("day")
      precisionMonth._1.id should be("month")
      precisionYear._1.id should be("year")
    }

    "Each precision dimension have their output type, second must be long, minute must be date, others datetime" in {
      dateTimeDimension.precision("5s").typeOp should be(TypeOp.DateTime)
      dateTimeDimension.precision("10s").typeOp should be(TypeOp.DateTime)
      dateTimeDimension.precision("15s").typeOp should be(TypeOp.DateTime)
      dateTimeDimension.precision("second").typeOp should be(TypeOp.Long)
      dateTimeDimension.precision("minute").typeOp should be(TypeOp.Date)
      dateTimeDimension.precision("day").typeOp should be(TypeOp.DateTime)
      dateTimeDimension.precision("month").typeOp should be(TypeOp.DateTime)
      dateTimeDimension.precision("year").typeOp should be(TypeOp.DateTime)
      dateTimeDimension.precision(DateTimeField.TimestampPrecision.id).typeOp should be(TypeOp.Timestamp)
    }
  }
}
