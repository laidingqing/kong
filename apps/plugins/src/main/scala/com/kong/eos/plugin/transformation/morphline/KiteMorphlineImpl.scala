
package com.kong.eos.plugin.transformation.morphline

import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.kitesdk.morphline.api.{Command, MorphlineContext, Record}
import org.kitesdk.morphline.base.Compiler

case class KiteMorphlineImpl(config: Config, outputFieldsSchema: Array[StructField]) {

  private val morphlineContext: MorphlineContext = new MorphlineContext.Builder().build()

  private val collector: ThreadLocal[EventCollector] = new ThreadLocal[EventCollector]() {
    override def initialValue(): EventCollector = new EventCollector(outputFieldsSchema)
  }

  private val morphline: ThreadLocal[Command] = new ThreadLocal[Command]() {
    override def initialValue(): Command = new Compiler().compile(config, morphlineContext, collector.get())
  }

  def process(inputRecord: Record): Row = {
    val coll = collector.get()
    coll.reset()
    morphline.get().process(inputRecord)
    coll.row
  }
}