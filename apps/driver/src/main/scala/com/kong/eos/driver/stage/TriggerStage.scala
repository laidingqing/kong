
package com.kong.eos.driver.stage

import com.kong.eos.sdk.pipeline.output.Output
import com.kong.eos.sdk.utils.AggregationTime
import com.kong.eos.serving.core.models.policy.PhaseEnum
import com.kong.eos.serving.core.models.policy.trigger.TriggerModel
import com.kong.eos.driver.step.Trigger
import com.kong.eos.driver.writer.{TriggerWriterHelper, WriterOptions}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream

trait TriggerStage extends BaseStage {
  this: ErrorPersistor =>

  def triggersStreamStage(initSchema: StructType,
                          inputData: DStream[Row],
                          outputs: Seq[Output],
                          window: Long): Unit = {
    val triggersStage = triggerStage(policy.streamTriggers)
    val errorMessage = s"Something gone wrong executing the triggers stream for: ${policy.input.get.name}."
    val okMessage = s"Triggers Stream executed correctly."
    generalTransformation(PhaseEnum.TriggerStream, okMessage, errorMessage) {
      triggersStage
        .groupBy(trigger => (trigger.overLast, trigger.computeEvery))
        .foreach { case ((overLast, computeEvery), triggers) =>
          val groupedData = (overLast, computeEvery) match {
            case (None, None) => inputData
            case (Some(overL), Some(computeE))
              if (AggregationTime.parseValueToMilliSeconds(overL) == window) &&
                (AggregationTime.parseValueToMilliSeconds(computeE) == window) => inputData
            case _ => inputData.window(
              Milliseconds(
                overLast.fold(window) { over => AggregationTime.parseValueToMilliSeconds(over) }),
              Milliseconds(
                computeEvery.fold(window) { computeEvery => AggregationTime.parseValueToMilliSeconds(computeEvery) }))
          }
          TriggerWriterHelper.writeStream(triggers, streamTemporalTable(policy.streamTemporalTable), outputs,
            groupedData, initSchema)
        }
    }
  }

  def triggerStage(triggers: Seq[TriggerModel]): Seq[Trigger] =
    triggers.map(trigger => createTrigger(trigger))

  private[driver] def createTrigger(trigger: TriggerModel): Trigger = {
    val okMessage = s"Trigger: ${trigger.name} created correctly."
    val errorMessage = s"Something gone wrong creating the trigger: ${trigger.name}. Please re-check the policy."
    generalTransformation(PhaseEnum.Trigger, okMessage, errorMessage) {
      Trigger(
        trigger.name,
        trigger.sql,
        trigger.overLast,
        trigger.computeEvery,
        WriterOptions(
          trigger.writer.outputs,
          trigger.writer.saveMode,
          trigger.writer.tableName,
          getAutoCalculatedFields(trigger.writer.autoCalculatedFields),
          trigger.writer.primaryKey,
          trigger.writer.partitionBy
        ),
        trigger.configuration)
    }
  }

  private[driver] def streamTemporalTable(policyTableName: Option[String]): String =
    policyTableName.flatMap(tableName => if (tableName.nonEmpty) Some(tableName) else None)
      .getOrElse("stream")

}
