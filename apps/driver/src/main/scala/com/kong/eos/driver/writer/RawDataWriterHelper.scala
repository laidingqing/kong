
package com.kong.eos.driver.writer

import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.utils.AggregationTime
import com.kong.eos.driver.factory.SparkContextFactory
import com.kong.eos.driver.step.RawData
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.dstream.DStream


object RawDataWriterHelper {

  def writeRawData(rawData: RawData, outputs: Seq[Output], input: DStream[Row]): Unit = {
    val RawSchema = StructType(Seq(
      StructField(rawData.timeField, TimestampType, nullable = false),
      StructField(rawData.dataField, StringType, nullable = true)))
    val eventTime = AggregationTime.millisToTimeStamp(System.currentTimeMillis())

    input.map(row => Row.merge(Row(eventTime), row))
      .foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          val rawDataFrame = SparkContextFactory.sparkSessionInstance.createDataFrame(rdd, RawSchema)

          WriterHelper.write(rawDataFrame, rawData.writerOptions, Map.empty[String, String], outputs)
        }
      })
  }
}
