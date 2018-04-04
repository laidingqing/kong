
package com.kong.eos.plugin.output.avro

import java.sql.Timestamp
import java.time.Instant

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.plugin.TemporalSparkContext
import com.kong.eos.sdk.pipeline.output.SaveModeEnum
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.io.File
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class AvroOutputIT extends TemporalSparkContext with Matchers {

  trait CommonValues {
    val tmpPath: String = File.makeTemp().name
    val sparkSession = SparkSession.builder().config(sc.getConf).getOrCreate()
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("minute", LongType)
    ))

    val data =
      sparkSession.createDataFrame(sc.parallelize(Seq(
        Row("Kevin", Random.nextInt, Timestamp.from(Instant.now).getTime),
        Row("Kira", Random.nextInt, Timestamp.from(Instant.now).getTime),
        Row("Ariadne", Random.nextInt, Timestamp.from(Instant.now).getTime)
      )), schema)
  }

  trait WithEventData extends CommonValues {
    val properties = Map("path" -> tmpPath)
//    val output = new AvroOutput("avro-test", properties)
  }


  "AvroOutput" should "throw an exception when path is not present" in {
    an[Exception] should be thrownBy new AvroOutput("avro-test", Map.empty)
  }

  it should "throw an exception when empty path " in {
//    an[Exception] should be thrownBy new AvroOutput("avro-test", Map("path" -> "    "))
  }

  it should "save a dataframe " in new WithEventData {

  }

}


