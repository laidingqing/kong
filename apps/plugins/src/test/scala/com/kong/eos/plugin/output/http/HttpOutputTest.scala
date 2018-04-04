
package com.kong.eos.plugin.output.http

import com.kong.eos.plugin.TemporalSparkContext
import com.kong.eos.sdk.pipeline.output.OutputFormatEnum
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpOutputTest extends TemporalSparkContext with Matchers {

  val properties = Map(
    "url" -> "https://httpbin.org/post",
    "delimiter" -> ",",
    "parameterName" -> "thisIsAKeyName",
    "readTimeOut" -> "5000",
    "outputFormat" -> "ROW",
    "postType" -> "body",
    "connTimeout" -> "6000"
  )

  val fields = StructType(StructField("name", StringType, false) ::
    StructField("age", IntegerType, false) ::
    StructField("year", IntegerType, true) :: Nil)
  val OkHTTPResponse = 200

  "An object of type RestOutput " should "have the same values as the properties Map" in {
    val rest = new HttpOutput("key", properties)

    rest.outputFormat should be(OutputFormatEnum.ROW)
    rest.readTimeout should be(5000)
  }
  it should "throw a NoSuchElementException" in {
    val properties2 = properties.updated("postType", "vooooooody")
    a[NoSuchElementException] should be thrownBy {
      new HttpOutput("keyName", properties2)
    }
  }

  /* DataFrame generator */
  private def dfGen(): DataFrame = {
    val sqlCtx = SparkSession.builder().config(sc.getConf).getOrCreate()
    val dataRDD = sc.parallelize(List(("user1", 23, 1993), ("user2", 26, 1990))).map { case (name, age, year) =>
      Row(name, age, year)
    }
    sqlCtx.createDataFrame(dataRDD, fields)
  }

  val restMock1 = new HttpOutput("key", properties)
  "Given a DataFrame it" should "be parsed and send through a Raw data POST request" in {

    dfGen().collect().foreach(row => {
      assertResult(OkHTTPResponse)(restMock1.sendData(row.mkString(restMock1.delimiter)).code)
    })
  }

  it should "return the same amount of responses as rows in the DataFrame" in {
    val size = dfGen().collect().map(row => restMock1.sendData(row.mkString(restMock1.delimiter)).code).size
    assertResult(dfGen().count())(size)
  }

  val restMock2 = new HttpOutput("key", properties.updated("postType", "parameter"))
  it should "be parsed and send as a POST request along with a parameter stated by properties.parameterKey " in {
    dfGen().collect().foreach(row => {
      assertResult(OkHTTPResponse)(restMock2.sendData(row.mkString(restMock2.delimiter)).code)
    })
  }

  val restMock3 = new HttpOutput("key", properties.updated("outputFormat", "JSON"))
  "Given a DataFrame it" should "be sent as JSON through a Raw data POST request" in {

    dfGen().toJSON.collect().foreach(row => {
      assertResult(OkHTTPResponse)(restMock3.sendData(row).code)
    })
  }

  val restMock4 = new HttpOutput("key", properties.updated("postType", "parameter").updated("format", "JSON"))
  it should "sent as a POST request along with a parameter stated by properties.parameterKey " in {

    dfGen().toJSON.collect().foreach(row => {
      assertResult(OkHTTPResponse)(restMock4.sendData(row).code)
    })
  }
}