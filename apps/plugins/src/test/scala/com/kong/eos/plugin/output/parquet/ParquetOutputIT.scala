
package com.kong.eos.plugin.output.parquet

import com.github.nscala_time.time.Imports._
import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.reflect.io.File

@RunWith(classOf[JUnitRunner])
class ParquetOutputIT extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
  self: FlatSpec =>

  @transient var sc: SparkContext = _

  override def beforeAll {
    Logger.getRootLogger.setLevel(Level.ERROR)
    sc = ParquetOutputIT.getNewLocalSparkContext(1, "test")
  }

  override def afterAll {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  trait CommonValues {

    val sqlContext = SparkSession.builder().config(sc.getConf).getOrCreate()
    import sqlContext.implicits._
    val time = DateTime.now.getMillis
    val data = sc.parallelize(Seq(Person("Kevin", 18, time), Person("Kira", 21, time), Person("Ariadne", 26, time))).toDS().toDF

    val tmpPath: String = File.makeTemp().name
  }

  trait WithEventData extends CommonValues {

    val properties = Map("path" -> tmpPath)
    val output = new ParquetOutput("parquet-test", properties)
  }

  trait WithoutGranularity extends CommonValues {

    val datePattern = "yyyy/MM/dd"
    val properties = Map("path" -> tmpPath, "datePattern" -> datePattern)
    val output = new ParquetOutput("parquet-test", properties)
    val expectedPath = "/0"
  }

  "ParquetOutputIT" should "save a dataframe" in new WithEventData {
    output.save(data, SaveModeEnum.Append, Map(Output.TableNameKey -> "person"))
    val read = sqlContext.read.parquet(s"$tmpPath/person").toDF
    read.count should be(3)
    read should be eq (data)
    File(tmpPath).deleteRecursively
    File("spark-warehouse").deleteRecursively
  }

  it should "throw an exception when path is not present" in {
    an[Exception] should be thrownBy new ParquetOutput("parquet-test", Map())
  }
}

object ParquetOutputIT {

  def getNewLocalSparkContext(numExecutors: Int = 1, title: String): SparkContext = {
    val conf = new SparkConf().setMaster(s"local[$numExecutors]").setAppName(title)
    SparkContext.getOrCreate(conf)
  }
}

case class Person(name: String, age: Int, minute: Long) extends Serializable
