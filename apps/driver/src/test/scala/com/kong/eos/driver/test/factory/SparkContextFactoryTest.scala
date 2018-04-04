
package com.kong.eos.driver.test.factory

import com.kong.eos.serving.core.config.KongCloudConfig
import com.kong.eos.serving.core.helpers.PolicyHelper
import com.kong.eos.driver.factory.SparkContextFactory
import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.Duration
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, _}

@RunWith(classOf[JUnitRunner])
class SparkContextFactoryTest extends FlatSpec with ShouldMatchers with BeforeAndAfterAll {
  self: FlatSpec =>

  override def afterAll {
    SparkContextFactory.destroySparkContext()
  }

  trait WithConfig {

    val config = KongCloudConfig.initConfig("sparta.local")
    val wrongConfig = ConfigFactory.empty
    val seconds = 6
    val batchDuraction = Duration(seconds)
    val specificConfig = Map("spark.driver.allowMultipleContexts" -> "true") ++
      PolicyHelper.getSparkConfFromProps(config.get)
  }

  "SparkContextFactorySpec" should "fails when properties is missing" in new WithConfig {
    an[Exception] should be thrownBy SparkContextFactory.sparkStandAloneContextInstance(
      Map.empty[String, String], Seq())
  }

  it should "create and reuse same context" in new WithConfig {
    val sc = SparkContextFactory.sparkStandAloneContextInstance(specificConfig, Seq())
    val otherSc = SparkContextFactory.sparkStandAloneContextInstance(specificConfig, Seq())
    sc should be equals (otherSc)
    SparkContextFactory.destroySparkContext()
  }

  it should "create and reuse same SparkSession" in new WithConfig {
    val sc = SparkContextFactory.sparkStandAloneContextInstance(specificConfig, Seq())
    val sqc = SparkContextFactory.sparkSessionInstance
    sqc shouldNot be equals (null)
    val otherSqc = SparkContextFactory.sparkSessionInstance
    sqc should be equals (otherSqc)
    SparkContextFactory.destroySparkContext()
  }

  it should "create and reuse same SparkStreamingContext" in new WithConfig {
    val checkpointDir = "checkpoint/SparkContextFactorySpec"
    val sc = SparkContextFactory.sparkStandAloneContextInstance(specificConfig, Seq())
    val ssc = SparkContextFactory.sparkStreamingInstance(batchDuraction, checkpointDir, None)
    ssc shouldNot be equals (None)
    val otherSsc = SparkContextFactory.sparkStreamingInstance(batchDuraction, checkpointDir, None)
    ssc should be equals (otherSsc)
  }
}
