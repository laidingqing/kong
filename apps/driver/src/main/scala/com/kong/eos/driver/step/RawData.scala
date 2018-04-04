
package com.kong.eos.driver.step

import java.io.Serializable

import com.kong.eos.driver.writer.WriterOptions


case class RawData(dataField: String,
                   timeField: String,
                   writerOptions: WriterOptions,
                   configuration: Map[String, Serializable])
