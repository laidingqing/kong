
package com.kong.eos.driver.writer

import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.driver.factory.SparkContextFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

object TransformationsWriterHelper {

  def writeTransformations(input: DStream[Row],
                           inputSchema: StructType,
                           outputs: Seq[Output],
                           writerOptions: WriterOptions): Unit = {
    input.foreachRDD(rdd =>
      if (!rdd.isEmpty()) {
        val transformationsDataFrame = SparkContextFactory.sparkSessionInstance.createDataFrame(rdd, inputSchema)

        WriterHelper.write(transformationsDataFrame, writerOptions, Map.empty[String, String], outputs)
      }
    )
  }
}
