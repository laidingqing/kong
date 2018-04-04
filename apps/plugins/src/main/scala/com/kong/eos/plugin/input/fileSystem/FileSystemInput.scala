
package com.kong.eos.plugin.input.fileSystem

import java.io.{Serializable => JSerializable}

import com.kong.eos.sdk.pipeline.input.Input
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import com.kong.eos.sdk.properties.ValidatingPropertyMap._


/**
  * This input creates a dataFrame given a path to an HDFS-compatible file.
  * Spark will monitor the directory and will only create dataFrames
  * from new entries.
  * @param properties
  */
class FileSystemInput(properties: Map[String, JSerializable]) extends Input(properties) {

  def initStream(ssc: StreamingContext, storageLevel: String): DStream[Row] = {

    ssc.textFileStream(properties.getString("directory", "")).map(data => Row(data))
  }
}
