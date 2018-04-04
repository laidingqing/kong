package com.kong.eos.plugin

import org.apache.spark._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}


private[plugin] trait TemporalSparkContext extends FlatSpec with BeforeAndAfterAll with BeforeAndAfter {

  val conf = new SparkConf()
    .setAppName("simulator-test")
    .setIfMissing("spark.master", "local[*]")

  @transient private var _sc: SparkContext = _
  @transient private var _ssc: StreamingContext = _

  def sc: SparkContext = _sc
  def ssc: StreamingContext = _ssc

  override def beforeAll()  {
    _sc = new SparkContext(conf)
    _ssc = new StreamingContext(sc, Seconds(2))
  }

  override def afterAll() : Unit = {
    if(ssc != null){
      ssc.stop(stopSparkContext =  false, stopGracefully = false)
      _ssc = null
    }
    if (sc != null){
      sc.stop()
      _sc = null
    }

    System.gc()
  }


}
