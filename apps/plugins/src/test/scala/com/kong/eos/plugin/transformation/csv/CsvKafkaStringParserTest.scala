package com.kong.eos.plugin.transformation.csv

import java.io.{Serializable => JSerializable}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpecLike}

//null,13950204677,192.168.0.16,1516344059693,/v1/upcoming/compare,1516344059679,1516344059693
@RunWith(classOf[JUnitRunner])
class CsvKafkaStringParserTest extends WordSpecLike with Matchers  {

  val CSV = "null,13950204677,192.168.0.16,1516344059693,/v1/upcoming/compare,1516344059679,15163440596"

  println(CSV.split(",").length)
}
