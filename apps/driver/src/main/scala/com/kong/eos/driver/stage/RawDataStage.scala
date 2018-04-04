
package com.kong.eos.driver.stage

import com.kong.eos.sdk.pipeline.output.{Output, SaveModeEnum}
import com.kong.eos.serving.core.models.policy.{PhaseEnum, RawDataModel}
import com.kong.eos.driver.step.RawData
import com.kong.eos.driver.writer.{RawDataWriterHelper, WriterOptions}
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

trait RawDataStage extends BaseStage {
  this: ErrorPersistor =>

  def saveRawData(rawModel: Option[RawDataModel], input: DStream[Row], outputs: Seq[Output]): Unit =
    if (rawModel.isDefined) {
      val rawData = rawDataStage()

      RawDataWriterHelper.writeRawData(rawData, outputs, input)
    }

  private[driver] def rawDataStage(): RawData = {
    val errorMessage = s"Something gone wrong saving the raw data. Please re-check the policy."
    val okMessage = s"RawData: created correctly."

    generalTransformation(PhaseEnum.RawData, okMessage, errorMessage) {
      require(policy.rawData.isDefined, "You need a raw data stage defined in your policy")
      require(policy.rawData.get.writer.tableName.isDefined, "You need a table name defined in your raw data stage")

      createRawData(policy.rawData.get)
    }
  }

  private[driver] def createRawData(rawDataModel: RawDataModel): RawData = {
    val okMessage = s"RawData created correctly."
    val errorMessage = s"Something gone wrong creating the RawData. Please re-check the policy."
    generalTransformation(PhaseEnum.RawData, okMessage, errorMessage) {
      RawData(
        rawDataModel.dataField,
        rawDataModel.timeField,
        WriterOptions(
          rawDataModel.writer.outputs,
          SaveModeEnum.Append,
          rawDataModel.writer.tableName,
          getAutoCalculatedFields(rawDataModel.writer.autoCalculatedFields),
          rawDataModel.writer.partitionBy,
          rawDataModel.writer.primaryKey
        ),
        rawDataModel.configuration)
    }
  }
}
